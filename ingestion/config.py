import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

load_dotenv()

@dataclass
class IngestionConfig:
    db_host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    db_port: str = field(default_factory=lambda: os.getenv("DB_PORT", "5432"))
    db_name: str = field(default_factory=lambda: os.getenv("DB_NAME", "stock_db"))
    db_user: str = field(default_factory=lambda: os.getenv("DB_USER", "airflow"))
    db_pass: str = field(default_factory=lambda: os.getenv("DB_PASS", "airflow"))
    provider: str = field(default_factory=lambda: os.getenv("PROVIDER", "vnstock").lower())
    # FALLBACK-ONLY: Used by MockProvider when provider=mock cannot call the API.
    # Production always uses VnstockProvider.get_vn30_symbols() → dynamic from API.
    # Verified against Listing(source='VCI').symbols_by_group('VN30') on 2026-06-24.
    symbols_pilot: List[str] = field(default_factory=lambda: [
        "ACB", "BID", "BSR", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "LPB",
        "MBB", "MSN", "MWG", "PLX", "SAB", "SHB", "SSB", "SSI", "STB", "TCB",
        "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VPL", "VRE"
    ])
    index_symbols: List[str] = field(default_factory=lambda: [
        "VNINDEX", "VN30"
    ])
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "5")))
    retry_max: int = field(default_factory=lambda: int(os.getenv("RETRY_MAX", "3")))

    @property
    def db_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"

# Single global config instance
config = IngestionConfig()

# Expose fields as module-level variables for backwards compatibility
DB_HOST = config.db_host
DB_PORT = config.db_port
DB_NAME = config.db_name
DB_USER = config.db_user
DB_PASS = config.db_pass
VN30_SYMBOLS = config.symbols_pilot
INDEX_SYMBOLS = config.index_symbols

def get_db_url() -> str:
    """Return the database connection string."""
    return config.db_url
