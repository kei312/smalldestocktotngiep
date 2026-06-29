import psycopg2
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator
from ta.volatility import BollingerBands

def run_verification():
    print("Connecting to database...")
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="stock_db",
        user="airflow",
        password="airflow"
    )
    
    query = """
    SELECT symbol, trade_date, close_price, 
           rsi_14, macd_line, macd_signal, macd_histogram,
           ma50, ma200, bb_upper, bb_lower
    FROM public_gold.fact_stock_indicators
    WHERE symbol IN ('ADS', 'AGG', 'AAA', 'VCB', 'HPG', 'VNM')
    ORDER BY symbol, trade_date
    """
    
    print("Fetching data from gold.fact_stock_indicators...")
    df = pd.read_sql(query, conn)
    conn.close()
    
    results = []
    
    for symbol, group in df.groupby('symbol'):
        group = group.sort_values('trade_date').reset_index(drop=True)
        
        import time
        start_time = time.time()
        # --- MACD & RSI ---
        rsi_indicator = RSIIndicator(close=group['close_price'], window=14)
        group['rsi_ta'] = rsi_indicator.rsi()
        
        macd_indicator = MACD(
            close=group['close_price'],
            window_fast=12,
            window_slow=26,
            window_sign=9
        )
        group['macd_line_ta'] = macd_indicator.macd()
        group['macd_signal_ta'] = macd_indicator.macd_signal()
        
        # --- MA & BB ---
        sma50_indicator = SMAIndicator(close=group['close_price'], window=50)
        group['ma50_ta'] = sma50_indicator.sma_indicator()
        
        sma200_indicator = SMAIndicator(close=group['close_price'], window=200)
        group['ma200_ta'] = sma200_indicator.sma_indicator()
        
        bb_indicator = BollingerBands(close=group['close_price'], window=20, window_dev=2)
        group['bb_upper_ta'] = bb_indicator.bollinger_hband()
        group['bb_lower_ta'] = bb_indicator.bollinger_lband()
        end_time = time.time()
        
        # Skip warm-up period (first 250 sessions)
        df_valid = group.iloc[250:].copy()
        
        # Absolute Errors
        df_valid['rsi_abs_err'] = (df_valid['rsi_14'] - df_valid['rsi_ta']).abs()
        df_valid['macd_line_abs_err'] = (df_valid['macd_line'] - df_valid['macd_line_ta']).abs()
        df_valid['macd_sig_abs_err'] = (df_valid['macd_signal'] - df_valid['macd_signal_ta']).abs()
        
        df_valid['ma50_abs_err'] = (df_valid['ma50'] - df_valid['ma50_ta']).abs()
        df_valid['ma200_abs_err'] = (df_valid['ma200'] - df_valid['ma200_ta']).abs()
        df_valid['bb_upper_abs_err'] = (df_valid['bb_upper'] - df_valid['bb_upper_ta']).abs()
        df_valid['bb_lower_abs_err'] = (df_valid['bb_lower'] - df_valid['bb_lower_ta']).abs()
        
        # MAPE
        df_valid['rsi_pe'] = np.where(df_valid['rsi_ta'] > 0.1, (df_valid['rsi_abs_err'] / df_valid['rsi_ta']) * 100, 0)
        df_valid['macd_line_pe'] = np.where(df_valid['macd_line_ta'].abs() > 0.01, (df_valid['macd_line_abs_err'] / df_valid['macd_line_ta'].abs()) * 100, 0)
        df_valid['macd_sig_pe'] = np.where(df_valid['macd_signal_ta'].abs() > 0.01, (df_valid['macd_sig_abs_err'] / df_valid['macd_signal_ta'].abs()) * 100, 0)
        
        df_valid['ma50_pe'] = np.where(df_valid['ma50_ta'] > 0.01, (df_valid['ma50_abs_err'] / df_valid['ma50_ta']) * 100, 0)
        df_valid['ma200_pe'] = np.where(df_valid['ma200_ta'] > 0.01, (df_valid['ma200_abs_err'] / df_valid['ma200_ta']) * 100, 0)
        df_valid['bb_upper_pe'] = np.where(df_valid['bb_upper_ta'] > 0.01, (df_valid['bb_upper_abs_err'] / df_valid['bb_upper_ta']) * 100, 0)
        df_valid['bb_lower_pe'] = np.where(df_valid['bb_lower_ta'] > 0.01, (df_valid['bb_lower_abs_err'] / df_valid['bb_lower_ta']) * 100, 0)
        
        mape = {
            'RSI 14': df_valid['rsi_pe'].mean(),
            'MACD Line': df_valid['macd_line_pe'].mean(),
            'MACD Signal': df_valid['macd_sig_pe'].mean(),
            'MA50': df_valid['ma50_pe'].mean(),
            'MA200': df_valid['ma200_pe'].mean(),
            'BB Upper': df_valid['bb_upper_pe'].mean(),
            'BB Lower': df_valid['bb_lower_pe'].mean()
        }
        
        max_err = {
            'RSI 14': df_valid['rsi_abs_err'].max(),
            'MACD Line': df_valid['macd_line_abs_err'].max(),
            'MACD Signal': df_valid['macd_sig_abs_err'].max(),
            'MA50': df_valid['ma50_abs_err'].max(),
            'MA200': df_valid['ma200_abs_err'].max(),
            'BB Upper': df_valid['bb_upper_abs_err'].max(),
            'BB Lower': df_valid['bb_lower_abs_err'].max()
        }
        
        print(f"\n=== Verification Results for {symbol} (Total post-warmup rows: {len(df_valid)}) ===")
        print(f"Time taken to calculate indicators using Python TA-Lib: {end_time - start_time:.4f} seconds")
        for key in mape.keys():
            print(f"{key:<15}: Max Abs Error = {max_err[key]:.6f}, MAPE = {mape[key]:.6f}%")
            
        results.append({
            'symbol': symbol,
            'mape': mape,
            'max_err': max_err
        })
        
    return results

if __name__ == "__main__":
    run_verification()
