# ADR-0001: Modular Go applications with manual dependency injection

- Status: Accepted
- Date: 2026-09-09

## Context

The portfolio needs multiple independently runnable workers without the operational
cost and cross-repository drift of many microservices. Domain correctness must remain
testable without infrastructure.

## Decision

Use one Go module with bounded contexts under `internal/` and small binaries under
`cmd/`. Dependencies point from infrastructure to application to domain. Wire
dependencies manually in each composition root until the graph demonstrates a need
for a DI framework.

## Consequences

The repository stays easy to run and review, while process boundaries remain explicit.
Manual wiring will grow as workers are added, but it is compile-time safe and visible.

## Revisit when

Wiring becomes repetitive across at least three applications, lifecycle ordering is
error-prone, or the dependency graph can no longer be understood from a composition
root.
