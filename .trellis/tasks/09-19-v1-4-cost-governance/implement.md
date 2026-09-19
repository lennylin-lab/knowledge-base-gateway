# Implementation plan: pricing and budgets

1. Add arithmetic, price selection, unknown-cost, and overflow tests.
2. Implement pricing/ledger/budget repositories and management mutations.
3. Add local and Redis atomic reservation/finalization with fault tests.
4. Integrate the common accounting gate into chat, responses, embeddings, and
   async creation/terminal handling without duplicating finalization logic.
5. Extend usage queries, errors, metrics hooks, configuration, and docs.
6. Run race, real PostgreSQL/Redis multi-instance, cancellation, outage, and
   migration tests plus all compatibility gates.

Rollback: disable monetary enforcement independently; continue ledger capture
where safe so operators retain evidence, then remove writers before schema down.
