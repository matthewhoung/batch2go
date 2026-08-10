"""The membership-attestation contract, asserted on the ONNX graph itself.

This is the permanent counter-fixture of ADR-0007. The failure it guards against
is silent: Triton scatters a batched output back to its requests along the batch
dimension, so a model that echoes its uid input returns each request its own uid
and nothing else. That output has the shape of evidence and the content of
nothing. These tests run the graph directly at batch size 4, where the
difference is visible, because at the unbatched entry every execution has one
member and the two graphs are indistinguishable.
"""

from __future__ import annotations

import numpy as np
import onnxruntime as ort
import pytest

from batch2go_modelgen.synthetic import ModelSpec, build_model, digest, serialize

BATCH = 4
PAYLOAD_FLOATS = 1024
KAPPA = 2


def session_for(spec: ModelSpec) -> ort.InferenceSession:
    options = ort.SessionOptions()
    options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    return ort.InferenceSession(serialize(build_model(spec)), options, providers=["CPUExecutionProvider"])


def run(spec: ModelSpec, uids: np.ndarray) -> dict[str, np.ndarray]:
    batch = uids.shape[0]
    session = session_for(spec)
    outputs = session.run(
        None,
        {
            "data": np.full((batch, spec.feature_width), 0.01, dtype=np.float32),
            "padding": np.full((batch, spec.payload_floats), 7.0, dtype=np.float32),
            "uid": uids,
        },
    )
    return {out.name: value for out, value in zip(session.get_outputs(), outputs)}


def attesting_spec(**kwargs) -> ModelSpec:
    return ModelSpec(kappa=KAPPA, payload_floats=PAYLOAD_FLOATS, **kwargs)


def test_every_batch_row_receives_the_complete_uid_set():
    uids = np.array([[11], [22], [33], [44]], dtype=np.int64)
    uid_set = run(attesting_spec(), uids)["uid_set"]

    assert uid_set.shape == (BATCH, BATCH)
    expected = set(uids.flatten().tolist())
    for row_index in range(BATCH):
        row = set(uid_set[row_index].tolist())
        assert row == expected, (
            f"row {row_index} attested {sorted(row)}, expected the full execution set "
            f"{sorted(expected)}"
        )


def test_naive_echo_fails_the_same_assertion():
    """The counter-fixture: an own-uid-only echo must not pass for evidence."""
    uids = np.array([[11], [22], [33], [44]], dtype=np.int64)
    uid_set = run(attesting_spec(attest=False), uids)["uid_set"]

    expected = set(uids.flatten().tolist())
    attesting_rows = [
        index for index in range(BATCH) if set(np.atleast_1d(uid_set[index]).tolist()) == expected
    ]
    assert not attesting_rows, (
        "the naive-echo model attested full membership; the assertion that is "
        "supposed to catch it no longer does"
    )

    # And it fails in the specific way the ADR describes: each row carries only
    # the uid it was given.
    for index in range(BATCH):
        assert np.atleast_1d(uid_set[index]).tolist() == [int(uids[index][0])]


@pytest.mark.parametrize("batch", [1, 2, 4, 8])
def test_attestation_holds_at_every_batch_size(batch: int):
    """The tile count is read at runtime, so one graph attests at any batch size.

    This is what makes membership evidence survive cross-cohort coalescing: the
    model reports the execution it was actually part of, not the one it expected.
    """
    uids = np.arange(1, batch + 1, dtype=np.int64).reshape(batch, 1) * 100
    uid_set = run(attesting_spec(), uids)["uid_set"]

    assert uid_set.shape == (batch, batch)
    expected = set(uids.flatten().tolist())
    for row_index in range(batch):
        assert set(uid_set[row_index].tolist()) == expected


def test_padding_input_is_consumed_and_cannot_be_optimized_away():
    """Payload has to be realized on every hop, so the input must stay live.

    If graph optimization could delete the padding input, the declared payload
    would stop crossing the wire and stop being staged to the device, and the
    fitted transport and compute-input components would silently omit it.
    """
    spec = attesting_spec()
    session = session_for(spec)
    input_names = {i.name for i in session.get_inputs()}
    assert "padding" in input_names, "the optimizer removed the padding input"

    # It is consumed but contributes nothing: two different payloads must give
    # bit-identical results.
    uids = np.array([[1], [2], [3], [4]], dtype=np.int64)
    data = np.full((BATCH, spec.feature_width), 0.01, dtype=np.float32)

    def infer(fill: float) -> np.ndarray:
        return session.run(
            None,
            {
                "data": data,
                "padding": np.full((BATCH, spec.payload_floats), fill, dtype=np.float32),
                "uid": uids,
            },
        )[0]

    np.testing.assert_array_equal(infer(0.0), infer(1234.5))


def test_generation_is_deterministic():
    """One spec, one digest — otherwise the catalog cannot pin the artifact."""
    spec = attesting_spec()
    first = serialize(build_model(spec))
    second = serialize(build_model(spec))
    assert digest(first) == digest(second)
    assert first == second


def test_attesting_and_echo_variants_are_different_artifacts():
    attesting = digest(serialize(build_model(attesting_spec())))
    echo = digest(serialize(build_model(attesting_spec(attest=False))))
    assert attesting != echo


def test_kappa_changes_the_graph():
    """κ is a graph parameter, so changing it must change the artifact."""
    k2 = digest(serialize(build_model(ModelSpec(kappa=2, payload_floats=PAYLOAD_FLOATS))))
    k4 = digest(serialize(build_model(ModelSpec(kappa=4, payload_floats=PAYLOAD_FLOATS))))
    assert k2 != k4
