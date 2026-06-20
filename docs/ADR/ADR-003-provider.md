# ADR-003: Unified VnstockProvider Architecture

## Status
Accepted

## Context
The original design might have implied using multiple provider classes for different data domains (e.g., historical prices, real-time prices, indices, fundamental data). This leads to code duplication, scattered configuration, and fragmented error handling logic.

## Decision
We decided to consolidate data fetching logic into a single `VnstockProvider` class that implements a generalized `DataProvider` interface. This single provider handles all API interactions with the `vnstock3` library.

## Consequences
- **Positive:** Centralized rate limiting, unified error handling and retry mechanisms. Simplifies the Airflow DAGs since they only need to interact with one unified client. Easier mockability in tests.
- **Negative:** The `VnstockProvider` class could grow large if we integrate too many domains. We mitigate this by keeping the interface clean and extracting common HTTP/API logic if necessary.
