import psycopg2
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

def generate_dashboard():
    # Connect to PostgreSQL
    conn_str = "postgresql://airflow:airflow@localhost:5432/stock_db"
    conn = psycopg2.connect(conn_str)
    
    # Read data
    print("Reading data from Gold layer...")
    df_market = pd.read_sql("SELECT * FROM gold.fact_market_summary ORDER BY trade_date", conn)
    df_hpg = pd.read_sql("SELECT * FROM gold.fact_stock_indicators WHERE symbol='HPG' ORDER BY trade_date", conn)
    df_hpg_price = pd.read_sql("SELECT * FROM gold.fact_stock_price WHERE symbol='HPG' ORDER BY trade_date", conn)
    
    conn.close()

    if df_hpg.empty:
        print("No data found for HPG, fallback to mock generation or check Airflow!")
        return
        
    print("Generating Plan B Dashboard...")
    # Create subplot layout
    fig = make_subplots(
        rows=4, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.05, 
        row_heights=[0.3, 0.3, 0.2, 0.2], 
        subplot_titles=(
            'Market Overview: VNINDEX & Gainers/Losers', 
            'HPG Close & Moving Averages', 
            'HPG RSI (14)', 
            'HPG MACD'
        )
    )

    # 1. Market Overview (VNINDEX Line + Gainers/Losers Bars)
    fig.add_trace(go.Scatter(x=df_market['trade_date'], y=df_market['vnindex'], name='VNINDEX', line=dict(color='purple')), row=1, col=1)
    # Gainers / Losers as bars
    fig.add_trace(go.Bar(x=df_market['trade_date'], y=df_market['gainers'], name='Gainers', marker_color='green'), row=1, col=1)
    fig.add_trace(go.Bar(x=df_market['trade_date'], y=-df_market['losers'], name='Losers', marker_color='red'), row=1, col=1)

    # 2. Close + MA
    fig.add_trace(go.Scatter(x=df_hpg['trade_date'], y=df_hpg['close'], name='HPG Close', line=dict(color='black')), row=2, col=1)
    fig.add_trace(go.Scatter(x=df_hpg['trade_date'], y=df_hpg['ma5'], name='MA5', line=dict(color='blue', dash='dot')), row=2, col=1)
    fig.add_trace(go.Scatter(x=df_hpg['trade_date'], y=df_hpg['ma20'], name='MA20', line=dict(color='orange')), row=2, col=1)

    # 3. RSI
    fig.add_trace(go.Scatter(x=df_hpg['trade_date'], y=df_hpg['rsi_14'], name='RSI 14', line=dict(color='purple')), row=3, col=1)
    fig.add_hline(y=70, line_dash='dash', line_color='red', row=3, col=1)
    fig.add_hline(y=30, line_dash='dash', line_color='green', row=3, col=1)

    # 4. MACD
    fig.add_trace(go.Scatter(x=df_hpg['trade_date'], y=df_hpg['macd_line'], name='MACD Line', line=dict(color='blue')), row=4, col=1)
    fig.add_trace(go.Scatter(x=df_hpg['trade_date'], y=df_hpg['macd_signal'], name='MACD Signal', line=dict(color='orange')), row=4, col=1)

    # Layout styling
    fig.update_layout(
        height=1200, 
        title_text='Plan B Backup Dashboard - Financial Data Overview',
        template='plotly_white',
        barmode='relative',
        hovermode='x unified'
    )
    
    os.makedirs('reports', exist_ok=True)
    out_file = 'reports/dashboard_backup.html'
    fig.write_html(out_file)
    print(f"Dashboard saved successfully to {out_file}")

if __name__ == '__main__':
    generate_dashboard()
