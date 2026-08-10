"""Deterministic construction of the synthetic ONNX model.

The graph has three jobs, and each one exists because of a specific way the
measurement could otherwise go wrong:

1. **Compute.** A repeated block, repeated κ times. κ is a dimensionless block
   count, not a milliseconds value: the same artifact digest has to serve the
   validation GPU and the confirmatory GPU, and no single graph holds
   milliseconds constant across both (ADR-0002).

2. **Payload.** The padding tensor is a declared *input consumed inside the
   model*, not something an adapter strips. That is what makes payload size real
   on every hop including host-to-device staging, in every cell — D0 has no
   adapter that could strip it (M1 §4). A no-op slice consumes it and folds a
   multiplied-by-zero residue into the output so that the graph optimizer cannot
   delete the input and quietly take the payload back out of the measurement.

3. **Membership.** The uid input is returned *tiled*, so every row of the output
   carries the complete uid set of its execution. This is the whole point of
   ADR-0007: Triton scatters a batched output back to requests along the batch
   dimension, so a model that merely echoes its uid input hands each request its
   own uid back — evidence-shaped, attesting nothing. `build_model` produces the
   attesting graph; `build_model(attest=False)` produces exactly that naive echo,
   which exists so the test suite can prove the assertion catches it.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper

#: Feature width of the compute path. Small and fixed: this slice calibrates
#: nothing, it only has to exercise the path.
FEATURE_WIDTH = 256

#: Opset the graph targets. Pinned, because a different opset is a different
#: artifact digest.
OPSET = 17

#: Fixed identity for the exporter, so serialization is byte-stable and the same
#: parameters always produce the same digest.
PRODUCER_NAME = "batch2go-modelgen"
PRODUCER_VERSION = "1"


@dataclass(frozen=True)
class ModelSpec:
    """The parameters that fully determine a synthetic model artifact."""

    kappa: int
    """Repeated-block count — the dimensionless compute-intensity parameter."""

    payload_floats: int
    """Width of the padding input, in float32 elements."""

    feature_width: int = FEATURE_WIDTH
    attest: bool = True
    """When false, build the naive-echo counter-fixture instead."""

    @property
    def name(self) -> str:
        kind = "synthetic" if self.attest else "naiveecho"
        return f"{kind}_k{self.kappa}_p{self.payload_floats}"


def deterministic_weights(rows: int, cols: int, seed: int) -> np.ndarray:
    """Generate reproducible weights without depending on any RNG implementation.

    A seeded numpy Generator would be reproducible only for as long as numpy's
    bit generator does not change; the artifact digest has to survive longer than
    that. This is a plain 64-bit LCG, so the bytes are a function of the spec and
    nothing else.
    """
    n = rows * cols
    state = np.uint64(seed * 2 + 1)
    multiplier = np.uint64(6364136223846793005)
    increment = np.uint64(1442695040888963407)

    out = np.empty(n, dtype=np.float64)
    with np.errstate(over="ignore"):
        for i in range(n):
            state = state * multiplier + increment
            # Take the high 32 bits: the low bits of an LCG are notoriously poor.
            out[i] = float(np.uint64(state) >> np.uint64(32))
    out = out / float(1 << 32) * 2.0 - 1.0
    # Scale so that repeated blocks neither saturate nor vanish through tanh.
    out /= np.sqrt(rows)
    return out.reshape(rows, cols).astype(np.float32)


def build_model(spec: ModelSpec) -> onnx.ModelProto:
    """Build the synthetic model described by spec."""
    width = spec.feature_width
    nodes = []
    initializers = []

    # ---- inputs: the three tensors every synthetic entry declares ----
    data = helper.make_tensor_value_info("data", TensorProto.FLOAT, ["N", width])
    padding = helper.make_tensor_value_info("padding", TensorProto.FLOAT, ["N", spec.payload_floats])
    uid = helper.make_tensor_value_info("uid", TensorProto.INT64, ["N", 1])

    # ---- compute: κ repeated blocks ----
    weights = numpy_helper.from_array(deterministic_weights(width, width, seed=1), name="W")
    bias = numpy_helper.from_array(
        deterministic_weights(1, width, seed=2).reshape(width), name="B"
    )
    initializers += [weights, bias]

    current = "data"
    for block in range(spec.kappa):
        matmul_out = f"mm_{block}"
        bias_out = f"bias_{block}"
        act_out = f"act_{block}"
        nodes += [
            helper.make_node("MatMul", [current, "W"], [matmul_out], name=f"block{block}_matmul"),
            helper.make_node("Add", [matmul_out, "B"], [bias_out], name=f"block{block}_add"),
            helper.make_node("Tanh", [bias_out], [act_out], name=f"block{block}_tanh"),
        ]
        current = act_out

    # ---- payload: a no-op slice that the optimizer cannot delete ----
    initializers += [
        numpy_helper.from_array(np.array([0], dtype=np.int64), name="pad_starts"),
        numpy_helper.from_array(np.array([1], dtype=np.int64), name="pad_ends"),
        numpy_helper.from_array(np.array([1], dtype=np.int64), name="pad_axes"),
        numpy_helper.from_array(np.array([0.0], dtype=np.float32), name="pad_zero"),
    ]
    nodes += [
        helper.make_node(
            "Slice",
            ["padding", "pad_starts", "pad_ends", "pad_axes"],
            ["pad_slice"],
            name="padding_noop_slice",
        ),
        # Multiplying by zero keeps the value out of the result while keeping the
        # input live: the padding still crosses every hop and is still staged to
        # the device, which is exactly the cost being measured.
        helper.make_node("Mul", ["pad_slice", "pad_zero"], ["pad_residue"], name="padding_zero"),
        helper.make_node("Add", [current, "pad_residue"], ["data_out"], name="payload_fold"),
    ]

    # ---- membership: the full uid set of the execution, to every member ----
    if spec.attest:
        initializers.append(numpy_helper.from_array(np.array([1], dtype=np.int64), name="tile_one"))
        initializers += [
            numpy_helper.from_array(np.array([0], dtype=np.int64), name="n_starts"),
            numpy_helper.from_array(np.array([1], dtype=np.int64), name="n_ends"),
            numpy_helper.from_array(np.array([0], dtype=np.int64), name="n_axes"),
        ]
        nodes += [
            # [N,1] -> [1,N]: one row holding the whole execution's uid set.
            helper.make_node("Transpose", ["uid"], ["uid_row"], perm=[1, 0], name="uid_transpose"),
            # The tile count is the execution's own batch size, read at runtime,
            # so the same graph attests correctly at any batch size — including
            # the cross-cohort coalescing this evidence exists to detect.
            helper.make_node("Shape", ["uid"], ["uid_shape"], name="uid_shape"),
            helper.make_node(
                "Slice",
                ["uid_shape", "n_starts", "n_ends", "n_axes"],
                ["uid_n"],
                name="uid_batch_size",
            ),
            helper.make_node("Concat", ["uid_n", "tile_one"], ["tile_repeats"], axis=0, name="uid_repeats"),
            helper.make_node("Tile", ["uid_row", "tile_repeats"], ["uid_set"], name="uid_tile"),
        ]
        uid_set = helper.make_tensor_value_info("uid_set", TensorProto.INT64, ["N", "N"])
    else:
        # The counter-fixture: each request gets only its own uid back. It looks
        # like membership evidence and attests nothing.
        nodes.append(helper.make_node("Identity", ["uid"], ["uid_set"], name="uid_naive_echo"))
        uid_set = helper.make_tensor_value_info("uid_set", TensorProto.INT64, ["N", 1])

    graph = helper.make_graph(
        nodes=nodes,
        name=spec.name,
        inputs=[data, padding, uid],
        outputs=[
            helper.make_tensor_value_info("data_out", TensorProto.FLOAT, ["N", width]),
            uid_set,
        ],
        initializer=initializers,
    )
    model = helper.make_model(
        graph,
        producer_name=PRODUCER_NAME,
        producer_version=PRODUCER_VERSION,
        opset_imports=[helper.make_opsetid("", OPSET)],
    )
    # ir_version is pinned rather than left to the installed onnx package, so an
    # onnx upgrade does not silently change the artifact digest.
    model.ir_version = 9
    onnx.checker.check_model(model)
    return model


def serialize(model: onnx.ModelProto) -> bytes:
    """Serialize deterministically, so one spec always yields one digest."""
    return model.SerializeToString(deterministic=True)


def digest(payload: bytes) -> str:
    """The artifact digest recorded in the catalog and verified before load."""
    return "sha256:" + hashlib.sha256(payload).hexdigest()
