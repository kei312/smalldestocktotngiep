import psycopg2
import pandas as pd
from ingestion.db import save_bronze_prices, get_connection

def test_idempotency():
    print("Starting Idempotency Test (Partial Write + Retry)...")
    
    # 1. Fetch 10 rows to use as our test data
    with get_connection() as conn:
        query = """
            SELECT code, date, open, high, low, close, volume, source, ingested_at
            FROM bronze.bronze_prices
            WHERE code = 'HPG'
            ORDER BY date DESC
            LIMIT 10
        """
        df = pd.read_sql(query, conn)
    
    if len(df) < 10:
        print("Not enough data to run the test. Need at least 10 rows of HPG.")
        return
        
    print(f"Fetched 10 rows for {df.iloc[0]['code']} from {df['date'].min()} to {df['date'].max()}")
    
    # 2. Delete these 10 rows to start with a clean slate
    delete_query = """
        DELETE FROM bronze.bronze_prices 
        WHERE code = %s AND date >= %s AND date <= %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(delete_query, (
                df.iloc[0]['code'], 
                df['date'].min(), 
                df['date'].max()
            ))
            deleted = cur.rowcount
            conn.commit()
            print(f"Deleted {deleted} rows from DB to start clean.")
            
    # 3. Simulate partial write (Crash halfway)
    print("\n--- SIMULATING PARTIAL WRITE (CRASH) ---")
    partial_df = df.head(5) # Only write 5 rows
    save_bronze_prices(partial_df)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM bronze.bronze_prices 
                WHERE code = %s AND date >= %s AND date <= %s
            """, (df.iloc[0]['code'], df['date'].min(), df['date'].max()))
            count_after_partial = cur.fetchone()[0]
            print(f"Row count after partial write: {count_after_partial} (Expected: 5)")
            
    # 4. Simulate Retry (Airflow retries the whole task)
    print("\n--- SIMULATING AIRFLOW RETRY ---")
    save_bronze_prices(df) # Write all 10 rows
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM bronze.bronze_prices 
                WHERE code = %s AND date >= %s AND date <= %s
            """, (df.iloc[0]['code'], df['date'].min(), df['date'].max()))
            count_after_retry = cur.fetchone()[0]
            print(f"Row count after retry: {count_after_retry} (Expected: 10)")
            
    if count_after_retry == 10:
        print("\nSUCCESS: Idempotency verified! Partial write + retry did not duplicate rows.")
    else:
        print("\nFAILED: Idempotency check failed.")

if __name__ == "__main__":
    test_idempotency()
