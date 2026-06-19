from api_request import mock_fetch_data
import psycopg2

# print(mock_fetch_data())

def connect_to_db():
    print("connecting")
    import os
    db_host = "db" if (os.environ.get("AIRFLOW_HOME") or os.path.exists("/.dockerenv")) else "localhost"
    db_port = "5432" if (os.environ.get("AIRFLOW_HOME") or os.path.exists("/.dockerenv")) else "5000"
    try:
        conn = psycopg2.connect(
            host=db_host,
            user="db_user",
            password="db_password",
            port=db_port,
            dbname="db"
        )
        print(conn)
        return conn
    except psycopg2.Error as e:
        print(f"database connection failed: {e}")
        raise

def create_table(conn):
    print("creating table if not exist...")
    
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                time TIMESTAMP PRIMARY KEY,
                open NUMERIC NOT NULL,
                high NUMERIC NOT NULL,
                low NUMERIC NOT NULL,
                close NUMERIC NOT NULL,
                volume BIGINT NOT NULL
            )
        """)
        conn.commit()
        print("Table created successfully")
                
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise
    
def insert_stock_data(conn, stock_data):
    try:
        cur = conn.cursor()        
        for data in stock_data:
            cur.execute("""
                INSERT INTO stock_data (time, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (time) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """, (
                data["time"],
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"]
            ))
        conn.commit()
        print("Data inserted successfully")
    except psycopg2.Error as e:
        print(f"Failed to insert data: {e}")
        raise

def main():
    try:
        data = mock_fetch_data()
        conn = connect_to_db()     
        create_table(conn)
        insert_stock_data(conn, data)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
            print("Database connection closed")


main()
    