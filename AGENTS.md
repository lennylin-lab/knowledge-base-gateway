<!-- TRELLIS:START -->
# Trellis Instructions

These instructions are for AI assistants working in this project.

This project is managed by Trellis. The working knowledge you need lives under `.trellis/`:

- `.trellis/workflow.md` — development phases, when to create tasks, skill routing
- `.trellis/spec/` — package- and layer-scoped coding guidelines (read before writing code in a given layer)
- `.trellis/workspace/` — per-developer journals and session traces
- `.trellis/tasks/` — active and archived tasks (PRDs, research, jsonl context)

If a Trellis command is available on your platform (e.g. `/trellis:finish-work`, `/trellis:continue`), prefer it over manual steps. Not every platform exposes every command.

If you're using Codex or another agent-capable tool, additional project-scoped helpers may live in:
- `.agents/skills/` — reusable Trellis skills
- `.codex/agents/` — optional custom subagents

Managed by Trellis. Edits outside this block are preserved; edits inside may be overwritten by a future `trellis update`.

<!-- TRELLIS:END -->

## Documentation

- **Repo-local (`docs/`)**: Store documentation that applies only to this repository under `docs/` (internal design, gateway runbooks, repo-specific specs). Keep cross-repo architecture and integration contracts in `../knowledge-base-docs/`.
- **Cross-workspace (`../knowledge-base-docs/`)**: Read `../knowledge-base-docs/architecture.md` when checking system topology or boundaries, and follow `../knowledge-base-docs/README.md` (and `.trellis/spec/guides/cross-workspace-docs-guide.md`) for document templates and `list.md` conventions.
  - **Incoming adaptations**: Check `../knowledge-base-docs/subsystems/gateway/list.md` for unchecked `- [ ]` entries and read the linked dated doc (`<YYYY-MM-DD>/<slug>.md`). Complete and verify the adaptation from those self-contained docs without reading sibling repos' source, then mark the item `- [x]` in `list.md`.
  - **Outgoing changes**: Whenever changes here alter `/v1/*` or `/admin/*` endpoints, wire envelopes, error codes, model capability/discovery fields (`embedding_dim`, `retrieval_profile`, default model slots), `GATEWAY_*` or provider credential env vars, PostgreSQL/Redis requirements, or container image/migration contracts affecting another subsystem (`server`, `deploy`, `database`), write a self-contained change note at `../knowledge-base-docs/subsystems/<target>/<YYYY-MM-DD>/<slug>.md` and add an unchecked `- [ ]` entry under `## <YYYY-MM-DD>` in `../knowledge-base-docs/subsystems/<target>/list.md` for every affected target.
  - **Boundary**: Describe only this repository's own changes, external contracts, compatibility, and verification steps in enough detail for consumers to adapt without reading this repo's source. Keep sibling repositories' source unread and leave their internal implementation changes to them.
