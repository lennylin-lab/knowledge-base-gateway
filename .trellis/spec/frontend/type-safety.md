# Type Safety

No frontend language, type system or runtime validation library is configured. Do not introduce TypeScript or generated client types without approved frontend scope. API contracts are JSON/HTTP contracts in `docs/agent-start.md`; future clients must validate untrusted responses at their boundary.

