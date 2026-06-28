import logging
import numpy as np
import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
import pandas as pd
from contextlib import contextmanager

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)

from .config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

logger = logging.getLogger(__name__)

@contextmanager
def get_connection():
    """Yield a database connection and ensure it is closed afterward."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        yield conn
    except Exception as e:
        logger.error("Failed to connect to database: %s", e)
        raise
    finally:
        if conn is not None:
            conn.close()

def save_bronze_prices(df: pd.DataFrame, table: str = "bronze.bronze_prices") -> None:
    """
    Save a DataFrame to the specified bronze table.
    Uses ON CONFLICT DO UPDATE to handle upserts.
    Uses execute_values for high performance batch inserts.
    """
    if df is None or df.empty:
        logger.warning("No data to save to %s", table)
        return

    # Copy to avoid mutating the original dataframe
    df = df.copy()
    
    # Ensure ingested_at exists
    if 'ingested_at' not in df.columns:
        df['ingested_at'] = pd.Timestamp.utcnow()

    columns = ["code", "date", "open", "high", "low", "close", "volume", "source", "ingested_at"]
    
    # Validate contract
    for col in columns:
        if col not in df.columns:
            logger.error("Missing required column in dataframe: %s", col)
            raise ValueError(f"Missing required column: {col}")

    # Dedup trước khi upsert — PostgreSQL báo CardinalityViolation nếu batch có 2 rows
    # cùng (code, date). Giữ dòng cuối (latest ingested_at) khi có trùng lặp.
    before = len(df)
    df = df.drop_duplicates(subset=["code", "date"], keep="last")
    if len(df) < before:
        logger.warning(
            "Dropped %d duplicate (code, date) rows before upsert (before=%d, after=%d)",
            before - len(df), before, len(df),
        )

    # Ensure native Python types for psycopg2 compatibility
    records = df[columns].to_records(index=False)
    data_tuples = [tuple(row) for row in records]

    insert_query = f"""
        INSERT INTO {table} (code, date, open, high, low, close, volume, source, ingested_at)
        VALUES %s
        ON CONFLICT (code, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            source = EXCLUDED.source,
            ingested_at = EXCLUDED.ingested_at;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                psycopg2.extras.execute_values(
                    cur,
                    insert_query,
                    data_tuples,
                    template=None,
                    page_size=1000
                )
                conn.commit()
                logger.info("Successfully inserted/upserted %d rows into %s", len(df), table)
            except Exception as e:
                conn.rollback()
                logger.error("Failed to insert data into %s: %s", table, e)
                raise

def save_bronze_vn30_components(symbols: list[str]) -> None:
    """
    Save the dynamic VN30 symbol list to bronze.bronze_vn30_components.
    Truncates the table before inserting the fresh list.
    """
    if not symbols:
        logger.warning("No VN30 symbols to save")
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                # Ensure table exists (safeguard)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bronze.bronze_vn30_components (
                        code VARCHAR(20) PRIMARY KEY,
                        ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("TRUNCATE TABLE bronze.bronze_vn30_components;")
                
                insert_query = """
                    INSERT INTO bronze.bronze_vn30_components (code)
                    VALUES (%s)
                    ON CONFLICT (code) DO NOTHING;
                """
                psycopg2.extras.execute_batch(cur, insert_query, [(sym,) for sym in symbols])
                conn.commit()
                logger.info("Successfully updated bronze.bronze_vn30_components with %d symbols", len(symbols))
            except Exception as e:
                conn.rollback()
                logger.error("Failed to save bronze_vn30_components: %s", e)
                raise

