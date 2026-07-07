import os
import json
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load env variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_db")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def query_to_dataframe(conn, sql, params=None):
    return pd.read_sql_query(sql, conn, params=params)

def normalize_price(p):
    """
    Standardize stock prices to Thousand VND (k VND).
    - If price > 500, it's in raw VND (from mock provider), divide by 1000 to get k VND.
    - Otherwise, it's already in k VND (from vnstock), keep it.
    """
    if p is None or pd.isna(p):
        return None
    return float(p) / 1000.0 if float(p) > 500 else float(p)

def normalize_index(idx):
    """
    Standardize indices (VNINDEX, VN30) to raw points.
    - If index > 5000, it's multiplied by 100 in the mock data, divide by 100.
    - Otherwise, it's raw points, keep it.
    """
    if idx is None or pd.isna(idx):
        return None
    return float(idx) / 100.0 if float(idx) > 5000 else float(idx)

def main():
    print("Connecting to database...")
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    print("Fetching data for Dashboard 1 (Market Overview)...")
    # 1.1 Latest market status
    latest_market_sql = """
        SELECT trade_date, gainers, losers, unchanged, total_symbols, total_volume, vnindex_close, vn30_close
        FROM public_gold.fact_market_summary
        ORDER BY trade_date DESC
        LIMIT 1;
    """
    df_latest_market = query_to_dataframe(conn, latest_market_sql)
    if df_latest_market.empty:
        print("No market summary data found!")
        conn.close()
        return

    latest_date = df_latest_market.iloc[0]['trade_date']
    print(f"Latest trading day: {latest_date}")

    # 1.2 VNINDEX line data & Historical Gainers/Losers/Volume
    vnindex_history_sql = """
        SELECT trade_date, vnindex_close, vn30_close, total_volume, gainers, losers, unchanged
        FROM public_gold.fact_market_summary
        WHERE trade_date >= (SELECT MAX(trade_date) FROM public_gold.fact_market_summary) - INTERVAL '400 days'
        ORDER BY trade_date ASC;
    """
    df_market_history = query_to_dataframe(conn, vnindex_history_sql)

    # Normalize Index columns in history and latest status
    df_latest_market['vnindex_close'] = df_latest_market['vnindex_close'].apply(normalize_index)
    df_latest_market['vn30_close'] = df_latest_market['vn30_close'].apply(normalize_index)
    
    df_market_history['vnindex_close'] = df_market_history['vnindex_close'].apply(normalize_index)
    df_market_history['vn30_close'] = df_market_history['vn30_close'].apply(normalize_index)

    # 1.3 Top Movers (with percent change calculated on normalized prices)
    # We fetch 400 days of VN30 for historical breadth, and 2 latest days for all stocks to calculate top movers.
    all_prices_sql = """
        SELECT p.symbol, p.trade_date, p.close_price, p.volume, COALESCE(s.is_vn30, FALSE) as is_vn30
        FROM public_gold.fact_stock_price p
        LEFT JOIN public_gold.dim_stock s ON p.symbol = s.symbol
        WHERE (s.is_vn30 = TRUE AND p.trade_date >= (SELECT MAX(trade_date) FROM public_gold.fact_stock_price) - INTERVAL '400 days')
           OR (p.trade_date IN (
               SELECT DISTINCT trade_date 
               FROM public_gold.fact_stock_price 
               ORDER BY trade_date DESC 
               LIMIT 2
           ))
        ORDER BY p.symbol, p.trade_date ASC;
    """
    df_all_prices = query_to_dataframe(conn, all_prices_sql)
    
    # Normalize price
    df_all_prices['close_normalized'] = df_all_prices['close_price'].apply(normalize_price)
    
    # Calculate previous close and percent change via pandas grouping
    df_all_prices['prev_close'] = df_all_prices.groupby('symbol')['close_normalized'].shift(1)
    df_all_prices['pct_change'] = ((df_all_prices['close_normalized'] - df_all_prices['prev_close']) / df_all_prices['prev_close'] * 100)
    df_all_prices['pct_change'] = df_all_prices['pct_change'].round(2)
    
    # Filter for the latest date
    df_latest_prices = df_all_prices[df_all_prices['trade_date'] == latest_date].copy()
    
    # Sort by percent change descending
    df_top_movers = df_latest_prices.sort_values(by='pct_change', ascending=False)

    print("Fetching data for Dashboard 2 (Stock Analysis)...")
    # 2.1 Indicators data for all symbols (filtered to last 400 days to optimize size)
    indicators_sql = """
        SELECT 
            ind.symbol, 
            ind.trade_date, 
            ind.close_price, 
            ind.ma50, 
            ind.ma200, 
            ind.bb_upper, 
            ind.bb_lower, 
            ind.rsi_14, 
            ind.macd_line, 
            ind.macd_signal, 
            ind.macd_histogram,
            COALESCE(s.is_vn30, FALSE) as is_vn30
        FROM public_gold.fact_stock_indicators ind
        LEFT JOIN public_gold.dim_stock s ON ind.symbol = s.symbol
        WHERE ind.trade_date >= (SELECT MAX(trade_date) FROM public_gold.fact_stock_indicators) - INTERVAL '400 days'
        ORDER BY ind.symbol, ind.trade_date ASC;
    """
    df_indicators = query_to_dataframe(conn, indicators_sql)

    conn.close()

    # Normalize technical indicator prices (close_price, ma50, ma200, bb_upper, bb_lower)
    for col in ['close_price', 'ma50', 'ma200', 'bb_upper', 'bb_lower']:
        df_indicators[col] = df_indicators[col].apply(normalize_price)

    # Pre-process Indicators Data for Javascript (grouped by symbol)
    # Convert dates to string for JSON serialization
    df_indicators['trade_date'] = df_indicators['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, (datetime, pd.Timestamp)) else str(x))
    df_market_history['trade_date'] = df_market_history['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, (datetime, pd.Timestamp)) else str(x))

    symbols = df_indicators['symbol'].unique().tolist()
    indicators_by_symbol = {}
    for sym in symbols:
        df_sym = df_indicators[df_indicators['symbol'] == sym]
        indicators_by_symbol[sym] = {
            'trade_date': df_sym['trade_date'].tolist(),
            'close_price': df_sym['close_price'].fillna('null').tolist(),
            'ma50': df_sym['ma50'].fillna('null').tolist(),
            'ma200': df_sym['ma200'].fillna('null').tolist(),
            'bb_upper': df_sym['bb_upper'].fillna('null').tolist(),
            'bb_lower': df_sym['bb_lower'].fillna('null').tolist(),
            'rsi_14': df_sym['rsi_14'].fillna('null').tolist(),
            'macd_line': df_sym['macd_line'].fillna('null').tolist(),
            'macd_signal': df_sym['macd_signal'].fillna('null').tolist(),
            'macd_histogram': df_sym['macd_histogram'].fillna('null').tolist(),
            'is_vn30': bool(df_sym['is_vn30'].iloc[0]) if not df_sym.empty else False
        }



    # 1.4 Top Movers parsing
    top_movers_list = []
    for idx, row in df_top_movers.iterrows():
        # Handle nan values for prev_close
        prev_close_val = None if pd.isna(row['prev_close']) else float(row['prev_close'])
        pct_change_val = 0.0 if pd.isna(row['pct_change']) else float(row['pct_change'])
        
        top_movers_list.append({
            'symbol': str(row['symbol']),
            'close_price': float(row['close_normalized']),
            'prev_close': prev_close_val,
            'pct_change': pct_change_val,
            'is_vn30': bool(row['is_vn30']),
            'volume': int(row['volume']) if not pd.isna(row['volume']) else 0
        })

    # Calculate historical stats for VN30 to support Dashboard 3 (Market Trends)
    df_vn30_prices = df_all_prices[df_all_prices['is_vn30'] == True].copy()
    vn30_history = df_vn30_prices.groupby('trade_date').agg(
        gainers=('pct_change', lambda x: int((x > 0).sum())),
        losers=('pct_change', lambda x: int((x < 0).sum())),
        unchanged=('pct_change', lambda x: int((x == 0).sum())),
        total_volume=('volume', lambda x: float(x.sum()) if not x.isna().all() else 0.0)
    ).reset_index()
    
    vn30_history = vn30_history.sort_values(by='trade_date')
    vn30_history_dates = vn30_history['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, (datetime, pd.Timestamp)) else str(x)).tolist()

    # Prepare data dict to embed in HTML
    data_payload = {
        'latest_market': {
            'trade_date': str(latest_date),
            'gainers': int(df_latest_market.iloc[0]['gainers']),
            'losers': int(df_latest_market.iloc[0]['losers']),
            'unchanged': int(df_latest_market.iloc[0]['unchanged']),
            'total_volume': float(df_latest_market.iloc[0]['total_volume']),
            'vnindex_close': float(df_latest_market.iloc[0]['vnindex_close']),
            'vn30_close': float(df_latest_market.iloc[0]['vn30_close']),
        },
        'market_history': {
            'trade_date': df_market_history['trade_date'].tolist(),
            'vnindex_close': df_market_history['vnindex_close'].tolist(),
            'vn30_close': df_market_history['vn30_close'].tolist(),
            'total_volume': df_market_history['total_volume'].astype(float).tolist(),
            'gainers': df_market_history['gainers'].tolist(),
            'losers': df_market_history['losers'].tolist(),
            'unchanged': df_market_history['unchanged'].tolist(),
        },
        'vn30_history': {
            'trade_date': vn30_history_dates,
            'total_volume': vn30_history['total_volume'].tolist(),
            'gainers': vn30_history['gainers'].tolist(),
            'losers': vn30_history['losers'].tolist(),
            'unchanged': vn30_history['unchanged'].tolist(),
        },
        'top_movers': top_movers_list,
        'symbols': symbols,
        'indicators_by_symbol': indicators_by_symbol
    }

    # HTML Template
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Vietnam Stock Analytics - Backup Dashboard</title>
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Google Fonts -->
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <!-- Plotly.js -->
    <script src="https://cdn.plot.ly/plotly-2.24.1.min.js"></script>
    <style>
        body {{
            background-color: #0f172a;
            color: #f8fafc;
            font-family: 'Outfit', sans-serif;
            padding-bottom: 50px;
        }}
        .header {{
            background: linear-gradient(135deg, #1e293b, #0f172a);
            border-bottom: 1px solid #334155;
            padding: 20px 0;
            margin-bottom: 30px;
        }}
        .card {{
            background-color: #1e293b;
            border: 1px solid #334155;
            border-radius: 12px;
            box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
            margin-bottom: 20px;
        }}
        .card-header {{
            background-color: #1e293b;
            border-bottom: 1px solid #334155;
            font-weight: 600;
            font-size: 1.1rem;
            color: #38bdf8;
        }}
        .nav-pills .nav-link {{
            color: #94a3b8;
            font-weight: 500;
            border-radius: 8px;
            transition: all 0.2s ease;
        }}
        .nav-pills .nav-link.active {{
            background-color: #0284c7;
            color: #ffffff;
        }}
        .nav-pills .nav-link:hover:not(.active) {{
            background-color: #334155;
            color: #f1f5f9;
        }}
        .kpi-card {{
            text-align: center;
            padding: 15px;
        }}
        .kpi-value {{
            font-size: 1.8rem;
            font-weight: 700;
            color: #ffffff;
        }}
        .kpi-label {{
            font-size: 0.85rem;
            color: #94a3b8;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-top: 5px;
        }}
        .table-custom {{
            color: #f8fafc;
        }}
        .table-custom th {{
            border-bottom: 2px solid #475569;
            color: #38bdf8;
            font-weight: 600;
        }}
        .table-custom td {{
            border-bottom: 1px solid #334155;
        }}
        .text-gainer {{
            color: #22c55e;
            font-weight: 600;
        }}
        .text-loser {{
            color: #ef4444;
            font-weight: 600;
        }}
        .text-unchanged {{
            color: #e2e8f0;
            font-weight: 600;
        }}
        .form-select {{
            background-color: #0f172a;
            border-color: #475569;
            color: #f8fafc;
        }}
        .form-select:focus {{
            background-color: #0f172a;
            color: #f8fafc;
            border-color: #0284c7;
            box-shadow: 0 0 0 0.25rem rgba(2, 132, 199, 0.25);
        }}
        .dashboard-tab {{
            display: none;
        }}
        .dashboard-tab.active {{
            display: block;
        }}
    </style>
</head>
<body>

    <header class="header">
        <div class="container">
            <div class="d-flex flex-wrap justify-content-between align-items-center">
                <div>
                    <h1 class="h3 mb-1 text-white fw-bold">Vietnam Stock Analysis</h1>
                    <p class="mb-0 text-muted">Hệ thống phân tích kỹ thuật và thị trường chuyên nghiệp</p>
                </div>
                <div class="text-end">
                    <span class="badge bg-danger px-3 py-2 fs-6">DỰ PHÒNG (PLAN B)</span>
                    <div class="text-muted mt-1 small">Phiên giao dịch gần nhất: <span id="latest-update-time"></span></div>
                </div>
            </div>
        </div>
    </header>

    <div class="container">
        <!-- Navigation Tabs & Global Group Filter -->
        <div class="row mb-4 align-items-center">
            <div class="col-md-8 col-12">
                <ul class="nav nav-pills justify-content-center justify-content-md-start mb-3 mb-md-0" id="dashboard-nav">
                    <li class="nav-item me-2 mb-2">
                        <button class="nav-link active" onclick="switchTab('market-overview')">Market Overview (DB 1)</button>
                    </li>
                    <li class="nav-item me-2 mb-2">
                        <button class="nav-link" onclick="switchTab('stock-analysis')">Stock Analysis (DB 2)</button>
                    </li>
                    <li class="nav-item me-2 mb-2">
                        <button class="nav-link" onclick="switchTab('market-trends')">Market Trends (DB 3)</button>
                    </li>
                </ul>
            </div>
            <div class="col-md-4 col-12 text-md-end text-center">
                <div class="d-inline-flex align-items-center bg-dark p-1 rounded border border-secondary">
                    <span class="text-info fw-bold me-2 ms-2 small">BỘ LỌC TOÀN CỤC:</span>
                    <div class="btn-group" role="group" aria-label="Global Group Filter">
                        <input type="radio" class="btn-check" name="global-group-filter" id="global-filter-all" autocomplete="off" checked onchange="onGlobalGroupChange('all')">
                        <label class="btn btn-sm btn-outline-info" for="global-filter-all">Tất cả</label>

                        <input type="radio" class="btn-check" name="global-group-filter" id="global-filter-vn30" autocomplete="off" onchange="onGlobalGroupChange('vn30')">
                        <label class="btn btn-sm btn-outline-info" for="global-filter-vn30">Chỉ VN30</label>
                    </div>
                </div>
            </div>
        </div>

        <!-- ==================== DASHBOARD 1: MARKET OVERVIEW ==================== -->
        <div id="market-overview" class="dashboard-tab active">
            <!-- Market Status Cards -->
            <div class="row">
                <div class="col-md-3 col-sm-6">
                    <div class="card kpi-card">
                        <div class="kpi-value text-info" id="kpi-vnindex">0.00</div>
                        <div class="kpi-label" id="kpi-vnindex-label">VN-Index</div>
                    </div>
                </div>
                <div class="col-md-3 col-sm-6">
                    <div class="card kpi-card">
                        <div class="kpi-value text-gainer" id="kpi-gainers">0</div>
                        <div class="kpi-label">Gainers (Số mã Tăng)</div>
                    </div>
                </div>
                <div class="col-md-3 col-sm-6">
                    <div class="card kpi-card">
                        <div class="kpi-value text-loser" id="kpi-losers">0</div>
                        <div class="kpi-label">Losers (Số mã Giảm)</div>
                    </div>
                </div>
                <div class="col-md-3 col-sm-6">
                    <div class="card kpi-card">
                        <div class="kpi-value text-warning" id="kpi-volume">0.00 M</div>
                        <div class="kpi-label">Latest Total Volume</div>
                    </div>
                </div>
            </div>

            <!-- Market Charts -->
            <div class="row">
                <div class="col-lg-8">
                    <div class="card">
                        <div class="card-header">Chỉ Số VNINDEX Lịch Sử</div>
                        <div class="card-body">
                            <div id="vnindex-chart" style="height: 400px;"></div>
                        </div>
                    </div>
                </div>
                <div class="col-lg-4">
                    <div class="card">
                        <div class="card-header" id="market-breadth-header">Độ Rộng Thị Trường (Ngày Gần Nhất)</div>
                        <div class="card-body">
                            <div id="market-breadth-pie" style="height: 400px;"></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Top Movers Table -->
            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">Top 10 Cổ Phiếu Biến Động Mạnh Nhất (Top Movers)</div>
                        <div class="card-body">
                            <div class="table-responsive">
                                <table class="table table-custom table-dark table-hover align-middle">
                                    <thead>
                                        <tr>
                                            <th>Mã Cổ Phiếu</th>
                                            <th>Giá Đóng Cửa (x1000 VND)</th>
                                            <th>Giá Trước Đó (x1000 VND)</th>
                                            <th>Thay Đổi (%)</th>
                                            <th>Trạng Thái</th>
                                        </tr>
                                    </thead>
                                    <tbody id="top-movers-body">
                                        <!-- Will be filled by JS -->
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- ==================== DASHBOARD 2: STOCK ANALYSIS ==================== -->
        <div id="stock-analysis" class="dashboard-tab">
            <!-- Symbol Selector -->
            <div class="row align-items-center mb-4">
                <div class="col-md-4">
                    <label for="symbol-select" class="form-label fw-bold text-info">Chọn mã cổ phiếu:</label>
                    <select class="form-select fs-5" id="symbol-select" onchange="onSymbolChange(this.value)">
                        <!-- Will be filled by JS -->
                    </select>
                </div>
                <div class="col-md-8 text-md-end mt-3 mt-md-0">
                    <h3 class="mb-0 text-white" id="current-analyzing-title">FPT - Phân Tích Kỹ Thuật</h3>
                </div>
            </div>

            <!-- Price & Moving Averages -->
            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">Biểu Đồ Giá Close & Trung Bình Động (MA50 / MA200) - Đơn vị: x1000 VND</div>
                        <div class="card-body">
                            <div id="price-ma-chart" style="height: 400px;"></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Bollinger Bands -->
            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">Bollinger Bands (BB) - Đơn vị: x1000 VND</div>
                        <div class="card-body">
                            <div id="bollinger-chart" style="height: 350px;"></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Indicators: RSI & MACD -->
            <div class="row">
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">RSI (14) - Chỉ Số Sức Mạnh Tương Đối</div>
                        <div class="card-body">
                            <div id="rsi-chart" style="height: 300px;"></div>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">MACD (Line, Signal, Histogram)</div>
                        <div class="card-body">
                            <div id="macd-chart" style="height: 300px;"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- ==================== DASHBOARD 3: MARKET TRENDS ==================== -->
        <div id="market-trends" class="dashboard-tab">
            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">Xu Hướng Gainers / Losers Lịch Sử (Market Trends)</div>
                        <div class="card-body">
                            <div id="gainers-losers-trend" style="height: 400px;"></div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">Thanh Khoản Thị Trường (Volume Giao Dịch)</div>
                        <div class="card-body">
                            <div id="market-volume-trend" style="height: 300px;"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>



    </div>

    <!-- JavaScript Data Payload & Rendering -->
    <script>
        const DATA = {json.dumps(data_payload)};

        document.getElementById('latest-update-time').innerText = DATA.latest_market.trade_date;

        const chartLayoutDefaults = {{
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            font: {{ color: '#94a3b8', family: 'Outfit, sans-serif' }},
            xaxis: {{
                gridcolor: '#334155',
                linecolor: '#475569',
                zerolinecolor: '#334155'
            }},
            yaxis: {{
                gridcolor: '#334155',
                linecolor: '#475569',
                zerolinecolor: '#334155'
            }},
            margin: {{ t: 30, r: 20, b: 40, l: 55 }},
            legend: {{ orientation: 'h', y: 1.1, x: 0 }}
        }};

        function filterSymbols(group) {{
            const select = document.getElementById('symbol-select');
            const currentValue = select.value;
            select.innerHTML = '';
            
            let filteredSymbols = [];
            if (group === 'vn30') {{
                filteredSymbols = DATA.symbols.filter(sym => {{
                    const sData = DATA.indicators_by_symbol[sym];
                    return sData && sData.is_vn30;
                }});
            }} else {{
                filteredSymbols = DATA.symbols;
            }}
            
            filteredSymbols.forEach(sym => {{
                const opt = document.createElement('option');
                opt.value = sym;
                opt.text = sym;
                select.appendChild(opt);
            }});
            
            if (filteredSymbols.includes(currentValue)) {{
                select.value = currentValue;
            }} else if (filteredSymbols.length > 0) {{
                if (filteredSymbols.includes('FPT')) {{
                    select.value = 'FPT';
                }} else {{
                    select.value = filteredSymbols[0];
                }}
                onSymbolChange(select.value);
            }}
        }}

        window.currentGroup = 'all';

        function onGlobalGroupChange(group) {{
            window.currentGroup = group;
            filterSymbols(group);
            renderMarketOverview();
            renderMarketTrends();
        }}

        window.addEventListener('load', () => {{
            onGlobalGroupChange('all');
        }});

        function switchTab(tabId) {{
            const tabs = document.querySelectorAll('.dashboard-tab');
            tabs.forEach(tab => tab.classList.remove('active'));

            document.getElementById(tabId).classList.add('active');

            const navLinks = document.querySelectorAll('#dashboard-nav .nav-link');
            navLinks.forEach(link => link.classList.remove('active'));

            event.target.classList.add('active');

            // Resize charts
            Plotly.Plots.resize('vnindex-chart');
            Plotly.Plots.resize('market-breadth-pie');
            Plotly.Plots.resize('price-ma-chart');
            Plotly.Plots.resize('bollinger-chart');
            Plotly.Plots.resize('rsi-chart');
            Plotly.Plots.resize('macd-chart');
            Plotly.Plots.resize('gainers-losers-trend');
            Plotly.Plots.resize('market-volume-trend');
            Plotly.Plots.resize('pe-compare-chart');
            Plotly.Plots.resize('roe-compare-chart');
        }}

        function renderMarketOverview() {{
            const lm = DATA.latest_market;
            const group = window.currentGroup || 'all';
            
            let gainersCount = lm.gainers;
            let losersCount = lm.losers;
            let unchangedCount = lm.unchanged;
            let totalVolume = lm.total_volume;
            
            if (group === 'vn30') {{
                const vn30Movers = DATA.top_movers.filter(item => item.is_vn30);
                gainersCount = vn30Movers.filter(item => item.pct_change > 0).length;
                losersCount = vn30Movers.filter(item => item.pct_change < 0).length;
                unchangedCount = vn30Movers.filter(item => item.pct_change === 0).length;
                totalVolume = vn30Movers.reduce((sum, item) => sum + (item.volume || 0), 0);
            }}

            document.getElementById('kpi-vnindex').innerText = (group === 'vn30' ? lm.vn30_close : lm.vnindex_close).toFixed(2);
            document.getElementById('kpi-vnindex-label').innerText = group === 'vn30' ? 'VN30 Index' : 'VN-Index';
            document.getElementById('kpi-gainers').innerText = gainersCount;
            document.getElementById('kpi-losers').innerText = losersCount;
            document.getElementById('kpi-volume').innerText = (totalVolume / 1000000).toFixed(2) + " M";

            const traceVnIndex = {{
                x: DATA.market_history.trade_date,
                y: DATA.market_history.vnindex_close,
                type: 'scatter',
                mode: 'lines',
                name: 'VN-Index',
                line: {{ color: '#0ea5e9', width: 2.5 }}
            }};
            const traceVn30 = {{
                x: DATA.market_history.trade_date,
                y: DATA.market_history.vn30_close,
                type: 'scatter',
                mode: 'lines',
                name: 'VN30-Index',
                line: {{ color: '#f59e0b', width: 1.5, dash: 'dot' }}
            }};
            
            const layoutVnIndex = JSON.parse(JSON.stringify(chartLayoutDefaults));
            layoutVnIndex.margin.t = 10;
            
            Plotly.newPlot('vnindex-chart', [traceVnIndex, traceVn30], layoutVnIndex);

            const tracePie = {{
                labels: ['Tăng (Gainers)', 'Giảm (Losers)', 'Không đổi'],
                values: [gainersCount, losersCount, unchangedCount],
                type: 'pie',
                marker: {{
                    colors: ['#22c55e', '#ef4444', '#64748b']
                }},
                textinfo: 'value+percent',
                textposition: 'inside',
                insidetextorientation: 'radial'
            }};
            const layoutPie = JSON.parse(JSON.stringify(chartLayoutDefaults));
            layoutPie.margin.t = 10;
            layoutPie.legend = {{ orientation: 'h', y: -0.1 }};
            Plotly.newPlot('market-breadth-pie', [tracePie], layoutPie);

            document.getElementById('market-breadth-header').innerText = group === 'vn30' ? 'Độ Rộng Thị Trường (VN30)' : 'Độ Rộng Thị Trường (Toàn Thị Trường)';

            const moversBody = document.getElementById('top-movers-body');
            moversBody.innerHTML = '';
            
            const filteredMovers = group === 'vn30' ? DATA.top_movers.filter(item => item.is_vn30) : DATA.top_movers;
            const displayMovers = filteredMovers.slice(0, 10);
            
            displayMovers.forEach(item => {{
                const tr = document.createElement('tr');
                const sign = item.pct_change > 0 ? '+' : '';
                const colorClass = item.pct_change > 0 ? 'text-gainer' : (item.pct_change < 0 ? 'text-loser' : 'text-unchanged');
                const statusBadge = item.pct_change > 0 ? '<span class="badge bg-success">TĂNG</span>' : (item.pct_change < 0 ? '<span class="badge bg-danger">GIẢM</span>' : '<span class="badge bg-secondary">ĐỨNG GIÁ</span>');
                
                tr.innerHTML = `
                    <td class="fw-bold text-white">${{item.symbol}}</td>
                    <td>${{item.close_price.toFixed(2)}}</td>
                    <td>${{item.prev_close ? item.prev_close.toFixed(2) : '-'}}</td>
                    <td class="${{colorClass}}">${{sign}}${{item.pct_change}}%</td>
                    <td>${{statusBadge}}</td>
                `;
                moversBody.appendChild(tr);
            }});
        }}

        function onSymbolChange(symbol) {{
            document.getElementById('current-analyzing-title').innerText = symbol + " - Phân Tích Kỹ Thuật";
            const sData = DATA.indicators_by_symbol[symbol];
            if (!sData) return;

            const traceClose = {{
                x: sData.trade_date,
                y: sData.close_price,
                type: 'scatter',
                mode: 'lines',
                name: 'Giá Close',
                line: {{ color: '#f8fafc', width: 2 }}
            }};
            const traceMA50 = {{
                x: sData.trade_date,
                y: sData.ma50,
                type: 'scatter',
                mode: 'lines',
                name: 'MA50',
                line: {{ color: '#eab308', width: 1.5 }}
            }};
            const traceMA200 = {{
                x: sData.trade_date,
                y: sData.ma200,
                type: 'scatter',
                mode: 'lines',
                name: 'MA200',
                line: {{ color: '#3b82f6', width: 1.5 }}
            }};
            const layoutPriceMA = JSON.parse(JSON.stringify(chartLayoutDefaults));
            layoutPriceMA.margin.t = 10;
            Plotly.newPlot('price-ma-chart', [traceClose, traceMA50, traceMA200], layoutPriceMA);

            const traceBBClose = {{
                x: sData.trade_date,
                y: sData.close_price,
                type: 'scatter',
                mode: 'lines',
                name: 'Close',
                line: {{ color: '#ffffff', width: 1.5 }}
            }};
            const traceBBUpper = {{
                x: sData.trade_date,
                y: sData.bb_upper,
                type: 'scatter',
                mode: 'lines',
                name: 'BB Upper',
                line: {{ color: '#a855f7', width: 1, dash: 'dash' }}
            }};
            const traceBBLower = {{
                x: sData.trade_date,
                y: sData.bb_lower,
                type: 'scatter',
                mode: 'lines',
                name: 'BB Lower',
                line: {{ color: '#a855f7', width: 1, dash: 'dash' }}
            }};
            const layoutBB = JSON.parse(JSON.stringify(chartLayoutDefaults));
            layoutBB.margin.t = 10;
            Plotly.newPlot('bollinger-chart', [traceBBClose, traceBBUpper, traceBBLower], layoutBB);

            const traceRsi = {{
                x: sData.trade_date,
                y: sData.rsi_14,
                type: 'scatter',
                mode: 'lines',
                name: 'RSI14',
                line: {{ color: '#ec4899', width: 1.8 }}
            }};
            const layoutRSI = JSON.parse(JSON.stringify(chartLayoutDefaults));
            layoutRSI.margin.t = 10;
            layoutRSI.shapes = [
                {{
                    type: 'line',
                    xref: 'paper',
                    x0: 0,
                    x1: 1,
                    yref: 'y',
                    y0: 70,
                    y1: 70,
                    line: {{ color: '#ef4444', width: 1, dash: 'dash' }}
                }},
                {{
                    type: 'line',
                    xref: 'paper',
                    x0: 0,
                    x1: 1,
                    yref: 'y',
                    y0: 30,
                    y1: 30,
                    line: {{ color: '#22c55e', width: 1, dash: 'dash' }}
                }}
            ];
            layoutRSI.yaxis.range = [10, 90];
            Plotly.newPlot('rsi-chart', [traceRsi], layoutRSI);

            const traceMacdLine = {{
                x: sData.trade_date,
                y: sData.macd_line,
                type: 'scatter',
                mode: 'lines',
                name: 'MACD Line',
                line: {{ color: '#0ea5e9', width: 1.5 }}
            }};
            const traceMacdSignal = {{
                x: sData.trade_date,
                y: sData.macd_signal,
                type: 'scatter',
                mode: 'lines',
                name: 'Signal Line',
                line: {{ color: '#f43f5e', width: 1.5 }}
            }};
            const traceMacdHist = {{
                x: sData.trade_date,
                y: sData.macd_histogram,
                type: 'bar',
                name: 'Histogram',
                marker: {{
                    color: sData.macd_histogram.map(v => v >= 0 ? '#22c55e' : '#ef4444')
                }}
            }};
            const layoutMACD = JSON.parse(JSON.stringify(chartLayoutDefaults));
            layoutMACD.margin.t = 10;
            Plotly.newPlot('macd-chart', [traceMacdLine, traceMacdSignal, traceMacdHist], layoutMACD);
        }}

        function renderMarketTrends() {{
            const group = window.currentGroup || 'all';
            const history = group === 'vn30' ? DATA.vn30_history : DATA.market_history;
            const titleG = group === 'vn30' ? 'VN30 Tăng (Gainers)' : 'Số Mã Tăng (Gainers)';
            const titleL = group === 'vn30' ? 'VN30 Giảm (Losers)' : 'Số Mã Giảm (Losers)';
            const titleVol = group === 'vn30' ? 'Thanh Khoản VN30 (Volume)' : 'Thanh Khoản Thị Trường (Volume)';

            const traceG = {{
                x: history.trade_date,
                y: history.gainers,
                type: 'scatter',
                mode: 'lines',
                name: titleG,
                line: {{ color: '#22c55e', width: 2 }},
                fill: 'tozeroy',
                fillcolor: 'rgba(34, 197, 94, 0.1)'
            }};
            const traceL = {{
                x: history.trade_date,
                y: history.losers,
                type: 'scatter',
                mode: 'lines',
                name: titleL,
                line: {{ color: '#ef4444', width: 2 }},
                fill: 'tozeroy',
                fillcolor: 'rgba(239, 68, 68, 0.1)'
            }};
            
            const layoutTrend = JSON.parse(JSON.stringify(chartLayoutDefaults));
            layoutTrend.margin.t = 10;
            Plotly.newPlot('gainers-losers-trend', [traceG, traceL], layoutTrend);

            const traceV = {{
                x: history.trade_date,
                y: history.total_volume,
                type: 'scatter',
                mode: 'lines',
                name: titleVol,
                line: {{ color: '#eab308', width: 2 }},
                fill: 'tozeroy',
                fillcolor: 'rgba(234, 179, 8, 0.1)'
            }};
            
            const layoutVol = JSON.parse(JSON.stringify(chartLayoutDefaults));
            layoutVol.margin.t = 10;
            Plotly.newPlot('market-volume-trend', [traceV], layoutVol);
        }}


    </script>
</body>
</html>
"""
    # Write to file
    output_dir = "reports"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "dashboard_backup.html")
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
        
    print(f"Success! Dashboard backup generated at: {output_path}")

if __name__ == "__main__":
    main()
