"""
Base provider interface and exception hierarchy.

All data providers must inherit from DataProvider and implement
the three abstract methods: get_prices, get_index, health_check.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Exception hierarchy (PROJECT_RULES.md Section 4)
# ---------------------------------------------------------------------------

class ProviderError(Exception):
    """Base exception for all provider-related errors."""
    pass


class ProviderRateLimitError(ProviderError):
    """Raised when the upstream API returns HTTP 429 or equivalent.

    This is a transient error — callers SHOULD retry with backoff.
    """
    pass


class ProviderTimeoutError(ProviderError):
    """Raised when a request to the upstream API times out.

    This is a transient error — callers SHOULD retry with backoff.
    """
    pass


class ProviderSchemaError(ProviderError):
    """Raised when the upstream API returns data with an unexpected schema.

    This is a NON-transient error — callers MUST NOT retry.
    Indicates a breaking change or data-contract violation on the provider side.
    """
    pass


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class DataProvider(ABC):
    """Abstract base class that every data provider must implement.

    Contract
    --------
    - ``get_prices``  → OHLCV DataFrame for one or more stock symbols.
    - ``get_index``   → OHLCV DataFrame for market indices (VNINDEX, VN30).
    - ``health_check`` → lightweight connectivity / availability test.

    All implementations must map upstream errors to the exception hierarchy
    defined above so that the ingestion layer can apply a uniform retry policy.
    """

    @abstractmethod
    def get_prices(
        self,
        symbols: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV data for the given stock symbols.

        Parameters
        ----------
        symbols : list[str]
            Stock ticker codes, e.g. ``["VNM", "VCB", "HPG"]``.
        start : date
            First trading date (inclusive).
        end : date
            Last trading date (inclusive).

        Returns
        -------
        pd.DataFrame
            Must contain at minimum the columns:
            ``code, date, open, high, low, close, volume, source``.

        Raises
        ------
        ProviderRateLimitError
            Upstream returned 429 / rate-limit equivalent.
        ProviderTimeoutError
            Request timed out.
        ProviderSchemaError
            Response schema does not match expected contract.
        ProviderError
            Any other provider-level failure.
        """
        ...

    @abstractmethod
    def get_index(
        self,
        indices: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV data for market indices.

        Parameters
        ----------
        indices : list[str]
            Index codes, e.g. ``["VNINDEX", "VN30"]``.
        start : date
            First trading date (inclusive).
        end : date
            Last trading date (inclusive).

        Returns
        -------
        pd.DataFrame
            Same column contract as ``get_prices``.

        Raises
        ------
        ProviderRateLimitError
        ProviderTimeoutError
        ProviderSchemaError
        ProviderError
        """
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """Return True if the provider is reachable and operational.

        Should be a fast, lightweight call (e.g. fetch 1 row of data
        or hit a status endpoint).  Used by Airflow's first DAG task
        to gate the rest of the pipeline.

        Returns
        -------
        bool
            ``True`` if healthy, ``False`` otherwise.
        """
        ...
