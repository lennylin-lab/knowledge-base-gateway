# Wire replay into CI and verify openai SDK compatibility

## Goal

Close the two remaining operational memos: run the offline replay command as
an explicit CI gate, and validate `/v1/responses` against a real openai-python
install so strict decoding behavior is verified rather than assumed.

## Background and Confirmed Facts

- `cmd/replay` (12 embedded deterministic fixtures) is documented in README
  and covered indirectly by `go test ./...`, but `.github/workflows/ci.yml`
  has no explicit replay step; the CI contract in quality-guidelines pins the
  migration-lifecycle and integration-step shapes.
- `/v1/responses` uses `DisallowUnknownFields`; the earlier check flagged that
  newer openai-python versions may inject default body fields and get 400s.
- A real SDK is available locally: `../knowledge-base-server/.venv` has
  openai-python 3.5.0 (system python has none); this task must not modify the
  sibling repository, only read-executing its venv binary is acceptable.
- The fake provider runs in dev mode with `GATEWAY_API_KEYS`/`GATEWAY_MODELS`;
  the SDK needs `base_url` pointing at the local gateway and a dev API key.

## Requirements

- Add a CI step that runs `go run ./cmd/replay` after the migration lifecycle
  (or alongside quality gates), failing the job on nonzero exit; keep the
  existing workflow structure and secret rules untouched.
- Run a compatibility pass with the real openai-python 3.5.0 from the
  sibling venv against a locally running gateway: chat completions
  (non-streaming and streaming) and `/v1/responses` (non-streaming,
  streaming, tools, structured output) via the SDK client; record every
  incompatibility (e.g. SDK-injected fields rejected by strict decoding).
- Fix any incompatibility inside this repository only: the acceptable
  resolution is either documenting the exact SDK configuration required, or
  narrowing strict decoding where it rejects standard SDK behavior without
  weakening validation (any decoder change keeps trailing-JSON rejection and
  is pinned by tests).
- Record the verified SDK version and required client configuration in
  `docs/developer-quickstart.md` (and the client contract if decoder
  behavior changes).
- No changes to `../knowledge-base-server` (read-only use of its venv).

## Acceptance Criteria

- [ ] CI workflow gains a replay step; the workflow stays actionlint-clean.
- [ ] A recorded pass/fail matrix exists (as task evidence) for chat
  non-streaming/streaming and responses non-streaming/streaming/tools/
  structured output with openai-python 3.5.0.
- [ ] Every SDK incompatibility is either fixed in this repo with tests or
  documented with the exact required client configuration.
- [ ] Trailing-JSON rejection and single-document decoding tests remain
  green; golden fixtures untouched.
- [ ] `gofmt`, `go vet`, `go test -race ./...` pass; env-gated tests run for
  real against `TEST_DATABASE_URL`/`TEST_REDIS_ADDR` (zero skips) if Go code
  changed.

## Out of Scope

- Python CI jobs or SDK-matrix automation (manual pass + documentation is
  the deliverable); changes to the sibling repository; new gateway endpoints.

## Key Decisions

- Lightweight task: PRD is the only planning artifact.
- Use the sibling venv read-only; if the pass requires a different SDK
  version, create a throwaway venv under /tmp instead of touching the
  sibling repo.

## Open Questions (blocking)

- None.
