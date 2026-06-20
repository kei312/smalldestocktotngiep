# ADR-002: Use dbt Core 1.10 instead of 2.0

## Status
Accepted

## Context
The data transformation layer (Silver and Gold) needs to be built with dbt. At the time of project initiation, dbt Core 1.x is widely adopted and stable, while dbt Core 2.x introduces breaking changes and may have fewer compatible packages and community-tested integrations.

## Decision
We decided to use dbt Core 1.10.x instead of adopting the newer dbt Core 2.x.

## Consequences
- **Positive:** High stability, guaranteed compatibility with `dbt-postgres` and existing macros, avoiding potential bleeding-edge bugs in production.
- **Negative:** We miss out on the latest features introduced in dbt 2.0, but this is a reasonable trade-off for a stable, predictable foundation.
