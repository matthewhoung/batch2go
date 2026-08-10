# batch2go — command facade.
#
# This file invokes; it never decides. Experiment matrices, model lifecycle
# loops, Triton configuration, and statistics live in the runner and its
# packages, because anything encoded here would be an experimental behavior
# chosen by a shell rather than by a manifest (CODEBASE.md §3).
#
# There is no CI/CD anywhere in this repository: every gate and suite is a local
# target, run by the author.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

GO ?= go
UV ?= uv
COMPOSE ?= docker compose
COMPOSE_FILE := deploy/compose.envv.yaml

# Pinned Triton image digest. A tag can be repointed; a digest cannot.
TRITON_DIGEST ?= sha256:f76300c73eba7dd4c67183210c10b3fef67fb9aa2c18f150cb916387078442f0
export TRITON_DIGEST

CATALOG      := artifacts/catalog.json
ARTIFACT_DIR := artifacts/generated
RESULTS_DIR  := results
MODELS_DIR   := $(RESULTS_DIR)/runtime-models
TRACES_DIR   := $(RESULTS_DIR)/triton-traces

# Walking-skeleton fixed parameters (spec 0001): one payload level, one κ level,
# cohort size 4, the local validation environment only.
KAPPA       ?= 8
PAYLOAD_MIB ?= 0.25
ARTIFACT_ID ?= synthetic_k$(KAPPA)_p65536

TRITON_ENDPOINT ?= 127.0.0.1:8001

.PHONY: help
help: ## List targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- build & test

BIN_DIR := bin

.PHONY: build
build: ## Build the data-plane and control-plane binaries into ./bin
	$(GO) build -o $(BIN_DIR)/ ./cmd/...

.PHONY: generate
generate: ## Regenerate protobuf code (committed, per CODEBASE.md §6)
	PATH="$$PATH:$$($(GO) env GOPATH)/bin" protoc -I. \
		--go_out=. --go_opt=module=github.com/matthewhoung/batch2go \
		api/events/v1/event.proto api/envelope/v1/envelope.proto
	PATH="$$PATH:$$($(GO) env GOPATH)/bin" protoc -I api/triton/v2 \
		--go_out=. --go_opt=module=github.com/matthewhoung/batch2go \
		--go_opt=Mgrpc_service.proto=github.com/matthewhoung/batch2go/api/triton/v2 \
		--go_opt=Mmodel_config.proto=github.com/matthewhoung/batch2go/api/triton/v2 \
		--go-grpc_out=. --go-grpc_opt=module=github.com/matthewhoung/batch2go \
		--go-grpc_opt=Mgrpc_service.proto=github.com/matthewhoung/batch2go/api/triton/v2 \
		--go-grpc_opt=Mmodel_config.proto=github.com/matthewhoung/batch2go/api/triton/v2 \
		grpc_service.proto model_config.proto

.PHONY: test
test: ## Unit and offline tests (no Triton required)
	$(GO) test ./...

.PHONY: test-models
test-models: ## Model generator tests, including the naive-echo counter-fixture
	$(UV) run --project tools/modelgen pytest tools/modelgen/tests -q

.PHONY: test-analysis
test-analysis: ## Analysis-side schema tests
	$(UV) run --project analysis pytest analysis/tests -q

.PHONY: bench-events
bench-events: ## Hot-path record benchmark (must report 0 allocs/op)
	$(GO) test ./internal/events/ -run '^$$' -bench BenchmarkRecord -benchmem

.PHONY: vet
vet: ## Static checks
	$(GO) vet ./...

# ------------------------------------------------------------------- artifacts

.PHONY: models
models: ## Generate synthetic model artifacts and the catalog manifest
	$(UV) run --project tools/modelgen python -m batch2go_modelgen \
		--out-dir $(ARTIFACT_DIR) --catalog $(CATALOG) \
		--kappa $(KAPPA) --payload-mib $(PAYLOAD_MIB)

.PHONY: repo
repo: ## Materialize the runtime Triton model repository from verified artifacts
	$(GO) run ./cmd/runner materialize \
		--catalog $(CATALOG) --artifact-dir $(ARTIFACT_DIR) \
		--runtime-dir $(MODELS_DIR) --artifact-id $(ARTIFACT_ID) --entries unbatched

# ------------------------------------------------------------------- the stack

.PHONY: stack-up
stack-up: models repo ## Bring up the validation stack (pinned Triton digest)
	@mkdir -p $(TRACES_DIR)
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --wait

.PHONY: stack-down
stack-down: ## Tear the stack down
	$(COMPOSE) -f $(COMPOSE_FILE) down -v --remove-orphans

.PHONY: stack-logs
stack-logs: ## Follow Triton's logs
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f

.PHONY: run-d0
run-d0: build ## Run the D0 diagnostic end to end and validate its bundle
	$(GO) run ./cmd/runner run --manifest experiments/manifests/d0-envv-b4.json \
		--image-digest $(TRITON_DIGEST)

.PHONY: run-f00
run-f00: build ## Run the F00 baseline end to end and validate its bundle
	$(GO) run ./cmd/runner run --manifest experiments/manifests/f00-envv-b4.json \
		--image-digest $(TRITON_DIGEST)

.PHONY: contracts
contracts: build ## The walking-skeleton acceptance suite: D0 and F00 at B=4, both bundles green
	@echo "== offline seam: validator self-test and defect fixtures =="
	$(GO) test ./internal/validate/... ./internal/events/... ./internal/testkit/...
	@echo
	@echo "== live seam: D0 =="
	$(GO) run ./cmd/runner run --manifest experiments/manifests/d0-envv-b4.json \
		--image-digest $(TRITON_DIGEST)
	$(GO) run ./cmd/runner validate --bundle $(RESULTS_DIR)/bundles/run-d0-envv-b4
	@echo
	@echo "== live seam: F00 =="
	$(GO) run ./cmd/runner run --manifest experiments/manifests/f00-envv-b4.json \
		--image-digest $(TRITON_DIGEST)
	$(GO) run ./cmd/runner validate --bundle $(RESULTS_DIR)/bundles/run-f00-envv-b4
	@echo
	@echo "contracts: D0 and F00 validated green at B=4"

.PHONY: validate
validate: ## Judge an archived bundle offline (BUNDLE=results/bundles/<run>)
	$(GO) run ./cmd/runner validate --bundle $(BUNDLE)

.PHONY: smoke
smoke: ## One request through the gateway; prints the verified uid attestation
	$(GO) run ./cmd/smoke \
		--endpoint $(TRITON_ENDPOINT) --catalog $(CATALOG) \
		--artifact-id $(ARTIFACT_ID) --entry unbatched

# --------------------------------------------------------------------- cleanup

.PHONY: clean
clean: ## Remove generated artifacts and run outputs
	rm -rf $(ARTIFACT_DIR) $(RESULTS_DIR) $(BIN_DIR)
