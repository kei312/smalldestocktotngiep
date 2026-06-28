# providers/registry.py
import os
import logging
from .base import DataProvider
from .vnstock_provider import VnstockProvider
from .mock_provider import MockProvider

logger = logging.getLogger(__name__)

# Module-level singleton cache
_provider_instance: DataProvider | None = None

def get_provider() -> DataProvider:
    """
    Factory function — returns a singleton DataProvider instance.
    Ensures RateLimiter and pause state are preserved across all callers.
    """
    global _provider_instance

    try:
        from ingestion.config import config
        provider_name = config.provider
    except ImportError:
        provider_name = os.getenv("PROVIDER", "vnstock").lower()

    # Return existing instance if already created for the same provider type
    if _provider_instance is not None:
        expected_type = VnstockProvider if provider_name == "vnstock" else MockProvider
        if isinstance(_provider_instance, expected_type):
            return _provider_instance

    # First-time instantiation
    if provider_name == "mock":
        logger.info("Using MockProvider (new instance).")
        _provider_instance = MockProvider()
    elif provider_name == "vnstock":
        logger.info("Using VnstockProvider (new instance).")
        _provider_instance = VnstockProvider()
    else:
        logger.warning("Unknown provider '%s'. Falling back to MockProvider.", provider_name)
        _provider_instance = MockProvider()

    return _provider_instance