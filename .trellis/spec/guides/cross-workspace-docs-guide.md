# Cross-Workspace Docs Guide

> **Purpose**: Keep cross-repo collaboration self-contained through `../knowledge-base-docs/` without reading sibling repositories' source code.

---

## Documentation Boundaries

| Scope | Location | Contents |
| --- | --- | --- |
| **Repo-local** | `docs/` (this repo) | Documentation applicable only to `knowledge-base-gateway` (internal design, gateway runbooks, repo-specific specs) |
| **Cross-workspace** | `../knowledge-base-docs/` | System architecture (`architecture.md`) and cross-subsystem change handoffs (`subsystems/<target>/<YYYY-MM-DD>/<slug>.md`) |

Authoritative rules, `list.md` structure, and the change-note template live in `../knowledge-base-docs/README.md`.

---

## Before Planning / Implementing (Incoming Handoffs)

- [ ] When starting integration or adaptation work, check `../knowledge-base-docs/subsystems/gateway/list.md` for unchecked `- [ ]` items.
- [ ] Read the linked dated docs under `../knowledge-base-docs/subsystems/gateway/<YYYY-MM-DD>/` and consult `../knowledge-base-docs/architecture.md` if system boundaries are involved.
- [ ] Design and implement the adaptation from those self-contained handoff docs without reading sibling repositories' source code.

---

## Before Check / Finish (Outgoing Changes & Completion)

- [ ] If this task adapted an incoming item from `../knowledge-base-docs/subsystems/gateway/list.md`, mark that checklist entry `- [x]` after tests pass.
- [ ] If this task changes an external contract affecting another subsystem (`server`, `panel`, `deploy`, `database`) — such as `/v1/*` or `/admin/*` endpoints, admin RBAC scopes, wire envelopes, error codes, model capability/discovery fields (`embedding_dim`, `retrieval_profile`, default model slots), `GATEWAY_*` or provider credential env vars, PostgreSQL/Redis storage dependencies (engine versions, extensions, roles/databases, migrations, Redis auth/persistence/key prefixes), or container image/migration contracts:
  - Write a self-contained change note at `../knowledge-base-docs/subsystems/<target>/<YYYY-MM-DD>/<slug>.md` per `../knowledge-base-docs/README.md`.
  - Add an unchecked `- [ ]` entry under `## <YYYY-MM-DD>` (newest date first) in `../knowledge-base-docs/subsystems/<target>/list.md` for every affected target.
  - **Storage dependency rule**: Because `knowledge-base-gateway`'s deployment orchestration (`docker-compose.yml`) no longer provisions PostgreSQL or Redis, **every** storage dependency change MUST be published to **both** `../knowledge-base-docs/subsystems/database/` and `../knowledge-base-docs/subsystems/deploy/` (with unchecked entries in both `list.md` files) so `database` and `deploy` handle the storage-side changes.
- [ ] **Strict boundary**: Describe only this repository's own changes, external contracts, compatibility, and verification steps in enough detail for consumers to adapt without reading this repo's source. Keep sibling repositories' source unread and leave their internal implementation changes to them.
