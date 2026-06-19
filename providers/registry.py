import os
import logging
from .base import DataProvider
from .vnstock_provider import VnstockProvider
from .mock_provider import MockProvider

logger = logging.getLogger(__name__)

def get_provider() -> DataProvider:
    """
    Factory function to instantiate the correct DataProvider
    based on the PROVIDER environment variable.
    """
    provider_name = os.getenv("PROVIDER", "vnstock").lower()
    
    if provider_name == "mock":
        logger.info("Using MockProvider.")
        return MockProvider()
    elif provider_name == "vnstock":
        logger.info("Using VnstockProvider.")
        return VnstockProvider()
    else:
        logger.warning(f"Unknown provider '{provider_name}'. Falling back to MockProvider.")
        return MockProvider()
