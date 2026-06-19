"""
ingestion/utils.py — Shared utilities for the ingestion layer.

Provides:
- setup_logging()        : Standard logging config for all ingestion scripts.
- retry()                : Decorator with exponential backoff + jitter.
- validate_dataframe()   : Schema + content validation against Bronze contract.
"""

import logging
import random
import time
from functools import wraps
from typing import Callable, Optional, Tuple, Type

import pandas as pd

from providers.base import ProviderError, ProviderRateLimitError, ProviderTimeoutError

logger = logging.getLogger(__name__)

# Required columns and their expected dtypes (Bronze contract — CONTEXT.md Section 3)
_REQUIRED_COLUMNS: Tuple[str, ...] = (
    "code", "date", "open", "high", "low", "close", "volume", "source"
)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with a uniform format for all ingestion scripts.

    Call once at the top of each entry-point script (fetch_prices.py, DAG, CLI).
    Subsequent calls are idempotent because basicConfig() is a no-op if
    handlers are already configured.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------

def retry(
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    jitter: float = 1.0,
    retry_on: Tuple[Type[Exception], ...] = (ProviderRateLimitError, ProviderTimeoutError),
) -> Callable:
    """Exponential backoff + jitter retry decorator.

    Parameters
    ----------
    max_attempts : int
        Total number of attempts (1 = no retry).
    backoff_base : float
        Base for the exponential backoff formula:
        ``wait = backoff_base ** (attempt - 1) + random.uniform(0, jitter)``
    jitter : float
        Upper bound of the random jitter added to each wait interval.
        Helps avoid thundering herd when multiple tasks retry simultaneously.
    retry_on : tuple[type[Exception], ...]
        Exception types that trigger a retry. Non-listed exceptions propagate
        immediately (e.g. ProviderSchemaError should never be retried).

    Example
    -------
    >>> @retry(max_attempts=3, retry_on=(ProviderRateLimitError, ProviderTimeoutError))
    ... def fetch():
    ...     ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        logger.error(
                            "%s failed after %d attempts. Last error: %s",
                            func.__name__,
                            max_attempts,
                            exc,
                        )
                        raise
                    wait = backoff_base ** (attempt - 1) + random.uniform(0, jitter)
                    logger.warning(
                        "%s attempt %d/%d failed (%s). Retrying in %.1fs…",
                        func.__name__,
                        attempt,
                        max_attempts,
                        type(exc).__name__,
                        wait,
                    )
                    time.sleep(wait)
                except Exception:
                    # Non-retryable — propagate immediately, do NOT swallow
                    raise
            raise last_exc  # unreachable but satisfies type checkers
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# DataFrame validation
# ---------------------------------------------------------------------------

def validate_dataframe(df: pd.DataFrame, context: str = "") -> pd.DataFrame:
    """Validate that a DataFrame conforms to the Bronze input contract.

    Checks
    ------
    1. DataFrame is not None or empty.
    2. All required columns are present.
    3. No null values in primary-key columns (code, date).
    4. Numeric OHLCV columns are positive.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to validate.
    context : str
        Descriptive label for log messages (e.g. "VNM 2024-01-01→2024-12-31").

    Returns
    -------
    pd.DataFrame
        The validated (unchanged) DataFrame if all checks pass.

    Raises
    ------
    ValueError
        On any validation failure. The message includes the failed check
        and the *context* string so callers can correlate log output.
    """
    prefix = f"[{context}] " if context else ""

    if df is None or df.empty:
        raise ValueError(f"{prefix}DataFrame is empty or None.")

    # 1. Required columns
    missing = set(_REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"{prefix}Missing required columns: {missing}")

    # 2. Null primary keys
    null_code = df["code"].isna().sum()
    null_date = df["date"].isna().sum()
    if null_code > 0 or null_date > 0:
        raise ValueError(
            f"{prefix}Null values in PK columns — code: {null_code}, date: {null_date}"
        )

    # 3. Positive OHLCV
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            non_positive = (df[col] <= 0).sum()
            if non_positive > 0:
                raise ValueError(
                    f"{prefix}Column '{col}' has {non_positive} non-positive value(s)."
                )

    logger.debug("%sDataFrame validated OK — %d rows.", prefix, len(df))
    return df
