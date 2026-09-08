# Engineering contract

Always load `golang-how-to` before Go coding, review, debugging, or setup work.

- Work diff-first and keep each change scoped to one roadmap slice.
- Preserve the dependency direction: infrastructure -> application -> domain.
- Keep financial decisions deterministic. AI output must never authorize, decline,
  post, reverse, or otherwise mutate a financial operation.
- Treat the immutable double-entry ledger as the financial source of truth.
- Use manual constructor injection at composition roots until the dependency graph
  demonstrates a need for a DI framework.
- Store money in minor units with an explicit currency; never use floating point.
- Validate financial invariants in domain code and back them with database constraints
  where PostgreSQL can enforce them.
- Design Kafka consumers for at-least-once delivery and idempotent processing.
- Add or update an ADR when a material architectural decision changes.
- Run cheap targeted checks first, then package/module tests, race detection, lint,
  vulnerability scanning, and integration checks in proportion to risk.
- Write code and technical artifacts in English. Report work to the user in Russian.
