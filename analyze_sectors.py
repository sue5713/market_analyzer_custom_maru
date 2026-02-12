import yfinance as yf
import pandas as pd
from tabulate import tabulate
import argparse
from datetime import datetime, timedelta
import pytz

# Define Sectors and Top 5 Constituents
SECTORS = {
    "XLK": ["AAPL", "NVDA", "MSFT", "AVGO", "CRM"],
    "XLV": ["LLY", "UNH", "JNJ", "ABBV", "MRK"],
    "XLF": ["BRK-B", "JPM", "V", "MA", "GS"],
    "XLY": ["AMZN", "TSLA", "HD", "MCD", "NKE"],
    "XLC": ["META", "GOOGL", "GOOG", "NFLX", "DIS"],
    "XLI": ["CAT", "GE", "UNP", "HON", "RTX"],
    "XLP": ["PG", "COST", "KO", "PEP", "WMT"],
    "XLE": ["XOM", "CVX", "COP", "SLB", "EOG"],
    "XLRE": ["PLD", "AMT", "EQIX", "WELL", "PSA"],
    "XLU": ["NEE", "SO", "DUK", "CEG", "SRE"],
    "XLB": ["LIN", "APD", "SHW", "CTVA", "FCX"]
}

SECTOR_NAMES = {
    "XLK": "情報技術", "XLV": "ヘルスケア", "XLF": "金融", "XLY": "一般消費財",
    "XLP": "生活必需品", "XLC": "通信", "XLE": "エネルギー", "XLI": "資本財",
    "XLB": "素材", "XLU": "公共事業", "XLRE": "不動産",
    "QQQ": "NAS100", "SPY": "S&P500", "DIA": "NYダウ"
}

INDICES = ["QQQ", "SPY", "DIA"]

def fetch_data():
    all_tickers = []
    for sector, stocks in SECTORS.items():
        all_tickers.append(sector)
        all_tickers.extend(stocks)
    all_tickers.extend(INDICES)
    
    all_tickers = list(set(all_tickers))
    print(f"Fetching data for {len(all_tickers)} tickers (1mo history, 15m interval)...")
    
    try:
        data = yf.download(all_tickers, period="1mo", interval="15m", group_by='ticker', auto_adjust=True, threads=True)
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def filter_data_by_date(df, start_dt=None, end_dt=None):
    if df is None or df.empty: return df
    filtered = df.copy()
    
    # Ensure index is timezone-aware (yfinance usually returns NY time, or UTC)
    # If not, assume NY
    if filtered.index.tzinfo is None:
        filtered.index = filtered.index.tz_localize("America/New_York")
    
    # Convert dataframe index to JST for easier comparison validation by user? 
    # Or convert Input (JST) to DataFrame's TZ (likely America/New_York)
    
    # Best practice: Convert everything to UTC for comparison
    df_utc = filtered.tz_convert("UTC")
    
    if start_dt:
        # Ensure start_dt is aware
        if start_dt.tzinfo is None:
             # If passed as naive, assume JST as per main() logic or handle gracefully
             start_dt = pytz.timezone('Asia/Tokyo').localize(start_dt)
        start_utc = start_dt.astimezone(pytz.UTC)
        df_utc = df_utc[df_utc.index >= start_utc]

    if end_dt:
        if end_dt.tzinfo is None:
             end_dt = pytz.timezone('Asia/Tokyo').localize(end_dt)
        end_utc = end_dt.astimezone(pytz.UTC)
        df_utc = df_utc[df_utc.index <= end_utc]
    
    # Return in original TZ (convert back from UTC to NY usually)
    return df_utc.tz_convert("America/New_York")

def analyze_last_day_shape(df, prev_close=None):
    if df.empty: return 0, "N/A", 0, 0, 0, 0, ""
    last_date = df.index[-1].date()
    last_day_df = df[df.index.date == last_date]
    if last_day_df.empty: return 0, "N/A", 0, 0, 0, 0, ""
        
    open_p = last_day_df.iloc[0]['Open']
    close_p = last_day_df.iloc[-1]['Close']
    high_p = last_day_df['High'].max()
    low_p = last_day_df['Low'].min()
    
    # Use Prev Close for % change if available, else Open (Intraday)
    base_p = prev_close if prev_close is not None else open_p
    move_pct = (close_p - base_p) / base_p * 100
    
    range_len = high_p - low_p
    date_str = last_date.strftime("%m/%d")
    if range_len == 0: return 0, "Doji", move_pct, open_p, high_p, close_p, date_str
    
    close_pos = (close_p - low_p) / range_len
    
    desc = ""
    score = 0 # -2 to +2
    
    if close_pos > 0.8:
        desc = "高値引け (Strong)"
        score = 2
    elif close_pos < 0.2:
        desc = "安値引け (Weak)"
        score = -2
    elif move_pct > 0.3:
        desc = "陽線 (Pos)"
        score = 1
    elif move_pct < -0.3:
        desc = "陰線 (Neg)"
        score = -1
    else:
        desc = "保ち合い (Neut)"
        score = 0
        
    return score, desc, move_pct, open_p, high_p, close_p, last_date.strftime("%m/%d")

def calculate_max_drawdown(df):
    """
    Calculate Maximum Drawdown (MDD). 
    MDD = Min((Price - Peak) / Peak)
    Returns percentage (e.g. -5.0 for 5% drop).
    """
    # Use Close price for drawdown calculation
    close_prices = df['Close']
    rolling_max = close_prices.cummax()
    drawdown = (close_prices - rolling_max) / rolling_max
    max_drawdown = drawdown.min()
    return max_drawdown * 100

def generate_three_scenarios(trend_return, last_score, last_move):
    """
    Generate 3 distinct scenarios: Good (Bull), Avg (Base), Bad (Bear).
    Returns: Grade, Scenarios Dict
    """
    grade = "普"
    scenarios = {
        "Good": "",
        "Avg": "",
        "Bad": ""
    }
    
    # 1. Strong Uptrend (>3%)
    if trend_return > 3.0:
        if last_score >= 1: # Uptrend + Strong
            grade = "良"
            scenarios["Avg"] = "上昇トレンド継続。高値更新を試す動き。"
            scenarios["Good"] = "勢いが加速し、帯状に上昇する (Band Walk)。"
            scenarios["Bad"] = "利益確定売りで一時的な調整が入る。"
        elif last_score <= -1: # Uptrend + Weak
            grade = "普"
            scenarios["Avg"] = "上昇一服。調整局面入りを示唆。"
            scenarios["Good"] = "押し目を形成し、再度上昇に転じる。"
            scenarios["Bad"] = "直近安値を割り込み、トレンドが崩れる。"
        else: # Uptrend + Neutral
            grade = "良"
            scenarios["Avg"] = "上昇トレンド継続。押し目待ち。"
            scenarios["Good"] = "もみ合いを上放れし、再加速する。"
            scenarios["Bad"] = "調整が長引き、レンジ相場へ移行する。"
            
    # 2. Strong Downtrend (<-3%)
    elif trend_return < -3.0:
        if last_score >= 1: # Downtrend + Strong
            grade = "普"
            scenarios["Avg"] = "自律反発。ショートカバー優勢。"
            scenarios["Good"] = "底打ちを確認し、本格的なリバウンドへ。"
            scenarios["Bad"] = "あくまで一時的な反発で、再度安値を更新。"
        elif last_score <= -1: # Downtrend + Weak
            grade = "悪"
            scenarios["Avg"] = "下落継続。安値模索の展開。"
            scenarios["Good"] = "セリングクライマックスを迎え、急反発する。"
            scenarios["Bad"] = "売りが売りを呼び、パニック的な下げになる。"
        else:
            grade = "悪"
            scenarios["Avg"] = "下落トレンド継続。戻り売り警戒。"
            scenarios["Good"] = "下げ止まり、底固めの動きへ。"
            scenarios["Bad"] = "ジリジリと下値を切り下げる。"
            
    # 3. Range / Neutral
    else:
        if last_score >= 1: # Range + Strong
            grade = "良"
            scenarios["Avg"] = "レンジ上限へのトライ。"
            scenarios["Good"] = "レンジを上抜け、新たな上昇トレンドへ。"
            scenarios["Bad"] = "レンジ上限で跳ね返され、再度保ち合いへ。"
        elif last_score <= -1: # Range + Weak
            grade = "悪"
            scenarios["Avg"] = "レンジ下限へのトライ。"
            scenarios["Good"] = "下限でサポートされ、反発する。"
            scenarios["Bad"] = "レンジを下抜け、下落トレンド入りする。"
        else:
            grade = "普"
            scenarios["Avg"] = "方向感なし。様子見。"
            scenarios["Good"] = "材料出現で動意づく。"
            scenarios["Bad"] = "出来高細り、閑散相場となる。"
            
    return grade, scenarios

def analyze_ticker(ticker, data, start_arg, end_arg):
    if ticker not in data.columns.levels[0]: return None
    raw = data[ticker].dropna()
    df = filter_data_by_date(raw, start_arg, end_arg)
    if df.empty: return None
    
    start_p = df.iloc[0]['Open']
    end_p = df.iloc[-1]['Close']
    high_p = df['High'].max()
    
    # Convert to JST for reporting
    jst = pytz.timezone('Asia/Tokyo')
    start_jst = df.index[0].astimezone(jst)
    end_jst = df.index[-1].astimezone(jst)
    
    start_date_str = start_jst.strftime("%m/%d %H:%M")
    end_date_str = end_jst.strftime("%m/%d %H:%M")
    
    # Calculate Previous Close for accurate Daily % Change
    prev_close = None
    try:
        current_date = df.index[-1].date()
        # Look at raw data strictly before the current day
        past_data = raw[raw.index.date < current_date]
        if not past_data.empty:
            prev_close = past_data.iloc[-1]['Close']
    except Exception:
        pass

    ret = (end_p - start_p) / start_p * 100
    
    # Calculate MDD and RF
    mdd = calculate_max_drawdown(df) # This is negative percent e.g. -5.0
    rf = 0.0
    # RF = Return / |MDD|
    if abs(mdd) > 0.001:
        rf = ret / abs(mdd)
    else:
        # If MDD is effectively 0 (only went up), RF is technically infinite.
        rf = 99.9

    score, desc, move, l_open, l_high, l_close, l_date = analyze_last_day_shape(df, prev_close)
    
    grade, scenarios = generate_three_scenarios(ret, score, move)
    
    return {
        "Ticker": ticker,
        "Start": start_p,
        "High": high_p,
        "End": end_p,
        "Return": ret,
        "MDD": mdd,
        "RF": rf,
        "DateRange": f"{start_date_str} - {end_date_str} JST",
        "LastScore": score,
        "LastDesc": desc,
        "LastMove": move,
        "LastOpen": l_open,
        "LastHigh": l_high,
        "LastClose": l_close,
        "LastDate": end_jst.strftime("%m/%d"), # Override with JST Date
        "Grade": grade,
        "Scenarios": scenarios
    }

def analyze_sector(sector_ticker, holdings, data, start_arg=None, end_arg=None):
    s_res = analyze_ticker(sector_ticker, data, start_arg, end_arg)
    if not s_res: return None
    
    stats = []
    
    # Force minimal 5 stocks? We have 5 in holdings.
    # We want to classify ALL 5 into Engine or Brake.
    # Logic: 
    #   Engine: Return > Sector Return (Leaders)
    #   Brake: Return <= Sector Return (Laggards)
    
    for stock in holdings:
        st_res = analyze_ticker(stock, data, start_arg, end_arg)
        if not st_res: continue
        
        rel_trend = st_res['Return'] - s_res['Return']
        role = "NEUTRAL"
        reason = ""
        
        # Forced Classification
        if rel_trend > 0:
            role = "ENGINE (牽引)"
            if st_res['LastScore'] >= 0:
                reason = f"トレンド牽引 (+{st_res['Return']:.1f}%)"
            else:
                reason = f"トレンドは強いが、直近で失速 ({st_res['LastDesc']})"
        else:
            role = "BRAKE (重石)"
            if st_res['LastScore'] > 0:
                reason = f"出遅れだが、直近は買われている ({st_res['LastDesc']})"
            else:
                reason = f"トレンドも直近も弱い ({st_res['LastDesc']})"
                
        st_res['Role'] = role
        st_res['Reason'] = reason
        stats.append(st_res)

    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        stats_df = stats_df.sort_values("Return", ascending=False)
        
    # Calculate Fund Quality (Breadth)
    # How many are Engines (Outperforming sector)?
    # If 4-5: Broad participation (Healthy)
    # If 1-2: Narrow participation (Selective)
    current_engines = [s for s in stats if "ENGINE" in s['Role']]
    engine_count = len(current_engines)
    total_count = len(stats)
    
    quality = "普通 (Mixed)"
    if total_count > 0:
        ratio = engine_count / total_count
        if ratio >= 0.8: # 4 or 5 out of 5
            quality = "健全な広がり (Healthy)"
        elif ratio <= 0.2: # 1 out of 5
            quality = "一部への逃避 (Selective)"
        elif ratio > 0.5:
            quality = "やや広い (Broad)"
        else:
            quality = "選別色あり (Mixed)"
    
    return {
        "sector": sector_ticker,
        "name": SECTOR_NAMES.get(sector_ticker, sector_ticker),
        "return": s_res['Return'],
        "start_p": s_res['Start'],
        "end_p": s_res['End'],
        "date_range": s_res['DateRange'],
        "last_desc": s_res['LastDesc'],
        "last_move": s_res['LastMove'],
        "last_date": s_res['LastDate'],
        "grade": s_res['Grade'],
        "quality": quality,
        "scenarios": s_res['Scenarios'],
        "stats": stats_df,
        "data": s_res
    }

def generate_narrative_report(results, index_results, start_dt, end_dt):
    report = []
    report.append("【天才投資家レポート】")
    report.append(f"分析期間: {start_dt.strftime('%Y-%m-%d %H:%M')} 〜 {end_dt.strftime('%Y-%m-%d %H:%M')} (JST)\n")
    
    # 1. Indices (Detailed)
    report.append("### ① 全体観 (Indices)")
    for idx_res in index_results:
        idx = idx_res['Ticker']
        name = SECTOR_NAMES.get(idx, idx)
        
        report.append(f"**{name} ({idx})**: {idx_res['Grade']}")
        report.append(f"  Price: {idx_res['Start']:.2f} -> {idx_res['End']:.2f} ({idx_res['Return']:+.2f}%) [{idx_res['DateRange']}]")
        report.append(f"  📊 リカバリー・ファクター (RF): {idx_res['RF']:.2f} | 最大ドローダウン (MDD): {idx_res['MDD']:.1f}%")
        report.append(f"  直近: {idx_res['LastDesc']} ({idx_res['LastMove']:+.1f}%) [{idx_res['LastDate']}]")
        
        # Drivers/Draggers Logic
        related_sectors = []
        if idx == "QQQ": related_sectors = ["XLK", "XLC", "XLY"]
        elif idx == "DIA": related_sectors = ["XLI", "XLF", "XLV"]
        else: related_sectors = ["XLK", "XLF", "XLV", "XLY", "XLI", "XLE"]
        
        drivers = []
        draggers = []
        
        for sec_ticker in related_sectors:
            if sec_ticker in results:
                sec_ret = results[sec_ticker]['return']
                sec_name = results[sec_ticker]['name']
                if sec_ret > idx_res['Return'] + 0.5:
                    drivers.append(f"- {sec_name}: {sec_ret:+.1f}%")
                elif sec_ret < idx_res['Return'] - 0.5:
                    draggers.append(f"- {sec_name}: {sec_ret:+.1f}%")
                    
        if drivers:
            report.append("🔥 **Engine (牽引)**:")
            report.extend(drivers)
        if draggers: 
            report.append("🧊 **Brake (重石)**:")
            report.extend(draggers)
        report.append("")

    report.append("="*40 + "\n")
    
    # 2. Sector Analysis
    sorted_secs = sorted(results.values(), key=lambda x: x['return'], reverse=True)
    winner = sorted_secs[0]
    loser = sorted_secs[-1]

    # Macro Conclusion
    # Determine Risk Sentiment based on Tech/ConsDisc vs Utilities/Staples
    risk_on_score = 0
    if "XLK" in results and "XLY" in results:
        risk_on_avg = (results["XLK"]["return"] + results["XLY"]["return"]) / 2
        risk_off_avg = 0
        count = 0
        if "XLU" in results: 
            risk_off_avg += results["XLU"]["return"]
            count += 1
        if "XLP" in results:
            risk_off_avg += results["XLP"]["return"]
            count += 1
        
        if count > 0:
            risk_off_avg /= count
            if risk_on_avg > risk_off_avg + 1.0:
                risk_on_score = 1 # Risk On
            elif risk_on_avg < risk_off_avg - 1.0:
                risk_on_score = -1 # Risk Off
    
    flow_desc = ""
    if risk_on_score == 1:
        flow_desc = "成長株への資金回帰が見られ、市場心理は「リスク選好 (Risk On)」です。"
    elif risk_on_score == -1:
        flow_desc = "ディフェンシブセクターへの逃避が見られ、市場心理は「リスク回避 (Risk Off)」です。"
    else:
        flow_desc = "セクター間の循環色が強く、方向感を探る展開です。"

    report.append("### ② マクロ結論: 資金流動")
    report.append(f"資金は**「{loser['name']}」から「{winner['name']}」へ**シフトしています。")
    report.append(f"【真実の眼】 {flow_desc}")
    report.append(f"勝者({winner['name']})は{winner['quality']}な買いが入っており、敗者({loser['name']})は資金流出が鮮明です。")
    report.append("\n" + "-"*20 + "\n")

    for res in sorted_secs:
        sec_name = res['name']
        ticker = res['sector']
        stats = res['stats']
        
        # Determine Breadth/Quality
        engines = stats[stats['Role'].str.contains('ENGINE')]
        brakes = stats[stats['Role'].str.contains('BRAKE')]
        
        report.append(f"## {sec_name} ({ticker})")
        report.append(f"**判定**: {res['grade']}")
        report.append(f"**資金の質の判定**: {res['quality']}")
        
        # Scenarios for Sector (Removed at user request)
        # sc = res['scenarios']
        # report.append("**想定シナリオ**:")
        # report.append(f"(普): {sc['Avg']}")
        # report.append(f"(良): {sc['Good']}")
        # report.append(f"(悪): {sc['Bad']}")
        
        report.append(f"**Price**: ${res['data']['Start']:.2f} -> ${res['data']['End']:.2f} ({res['return']:+.2f}%) [{res['data']['DateRange']}]")
        report.append(f"**📊 リカバリー・ファクター (RF)**: {res['data']['RF']:.2f} | **最大ドローダウン (MDD)**: {res['data']['MDD']:.1f}%")
        report.append(f"**直近**: {res['data']['LastDesc']} [{res['data']['LastDate']}]")
        
        # Date range for individual lines (short format)
        short_date_range = f"[{start_dt.strftime('%m/%d')}-{end_dt.strftime('%m/%d')}]"
        
        if not engines.empty:
            report.append("🔥 **Engine (牽引)**:")
            for _, row in engines.iterrows():
                # Trend: Start->High->End (Return%) [Date] (Legend) [RF:...]
                trend_str = f"Trend: {row['Start']:.2f}->{row['High']:.2f}->{row['End']:.2f} ({row['Return']:+.1f}%) {short_date_range} (始値->高値->終値) [RF:{row['RF']:.2f}]"
                
                # Last: Open->High->Close (Move%) [Date] (Legend)
                last_str = f"Last: {row['LastOpen']:.2f}->{row['LastHigh']:.2f}->{row['LastClose']:.2f} ({row['LastMove']:+.1f}%) [{row['LastDate']}] (始値->高値->終値)"
                
                report.append(f"- {row['Ticker']}: {trend_str} / {last_str} -> {row['Reason']}")
        
        if not brakes.empty:
            report.append("🧊 **Brake (重石)**:")
            for _, row in brakes.iterrows():
                # Trend: Start->High->End (Return%) [Date] (Legend) [RF:...]
                trend_str = f"Trend: {row['Start']:.2f}->{row['High']:.2f}->{row['End']:.2f} ({row['Return']:+.1f}%) {short_date_range} (始値->高値->終値) [RF:{row['RF']:.2f}]"
                
                # Last: Open->High->Close (Move%) [Date] (Legend)
                last_str = f"Last: {row['LastOpen']:.2f}->{row['LastHigh']:.2f}->{row['LastClose']:.2f} ({row['LastMove']:+.1f}%) [{row['LastDate']}] (始値->高値->終値)"
                
                report.append(f"- {row['Ticker']}: {trend_str} / {last_str} -> {row['Reason']}")
        
        report.append("\n" + "-"*20 + "\n")

    # --- 3. RF Ranking Section ---
    report.append("### ③ リカバリー・ファクター (RF) ランキング")
    report.append("「リスクあたりのリターン効率」を比較します。数値が高いほど優秀です。\n")
    
    date_range_str = f"({start_dt.strftime('%m/%d')} - {end_dt.strftime('%m/%d')})"

    # Sector Ranking
    report.append(f"#### 【セクター別 RF ランキング】 {date_range_str}")
    # Sort sectors by RF descending
    sorted_sectors_rf = sorted(results.values(), key=lambda x: x['data']['RF'], reverse=True)
    for i, res in enumerate(sorted_sectors_rf, 1):
        rf_val = res['data']['RF']
        mdd_val = res['data']['MDD']
        ret_val = res['return']
        icon = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else f"{i}."
        report.append(f"{icon} **{res['name']} ({res['sector']})**: RF {rf_val:.2f} (Return: {ret_val:+.1f}% / MDD: {mdd_val:.1f}%)")
    
    report.append(f"\n#### 【銘柄別 RF ランキング (Top 10)】 {date_range_str}")
    # Collect all stocks from all stats
    all_stocks = []
    for res in results.values():
        if 'stats' in res and not res['stats'].empty:
            for _, row in res['stats'].iterrows():
                 all_stocks.append(row)
    
    # Sort stocks by RF descending
    # Convert list of Series to DataFrame for easier sorting if needed, but list sort is fine.
    # row is a pandas Series, so accessing by key is fine.
    sorted_stocks_rf = sorted(all_stocks, key=lambda x: x['RF'], reverse=True)
    
    # Top 10
    for i, stock in enumerate(sorted_stocks_rf[:10], 1):
        icon = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else f"{i}."
        report.append(f"{icon} **{stock['Ticker']}**: RF {stock['RF']:.2f} (Return: {stock['Return']:+.1f}% / MDD: {stock['MDD']:.1f}%)")
    
    report.append(f"\n#### 【銘柄別 RF ワースト (Bottom 5)】 {date_range_str}")
    # Bottom 5 (Worst RF)
    for i, stock in enumerate(sorted_stocks_rf[-5:], 1):
        # Reverse index for display? No, just list them.
        report.append(f"💀 **{stock['Ticker']}**: RF {stock['RF']:.2f} (Return: {stock['Return']:+.1f}% / MDD: {stock['MDD']:.1f}%)")
        
    report.append("\n" + "="*40 + "\n")

    return "\n".join(report)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str, help='Start datetime (YYYY-MM-DD HH:MM) in JST')
    parser.add_argument('--end', type=str, help='End datetime (YYYY-MM-DD HH:MM) in JST')
    parser.add_argument('--days', type=int, default=14)
    args = parser.parse_args()
    
    data = fetch_data()
    if data is None: return

    jst = pytz.timezone('Asia/Tokyo')
    
    # Default end is Now (JST)
    end_dt = datetime.now(jst)
    
    # Logic for customized time range
    if args.end:
        # User provides 'YYYY-MM-DD HH:MM' in JST
        try:
            # Parse argument as specific time
            local_dt = datetime.strptime(args.end, "%Y-%m-%d %H:%M")
            end_dt = jst.localize(local_dt)
        except ValueError:
            # Fallback for simple date
            end_dt = pd.to_datetime(args.end).tz_localize(jst)

    # Default start is end - days
    start_dt = end_dt - timedelta(days=args.days)
    
    if args.start:
        try:
            local_dt = datetime.strptime(args.start, "%Y-%m-%d %H:%M")
            start_dt = jst.localize(local_dt)
        except ValueError:
            start_dt = pd.to_datetime(args.start).tz_localize(jst)
        
    start_str = start_dt.strftime("%Y-%m-%d %H:%M")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M")
    
    # Convert JST datetimes to string for filter function (which checks against index)
    # The filter_data_by_date expects strings that pd.to_datetime can handle, or we can pass naive UTC?
    # Actually, yfinance index is usually America/New_York localized or UTC.
    # We should convert our JST range to the dataframe's timezone for accurate filtering.
    
    print(f"Analyzing {start_str} JST to {end_str} JST...")

    # We need to pass the actual datetime objects to filter or convert to string properly
    # Let's adjust filter_data_by_date to handle datetime objects directly to avoid confusion
    
    # Helper to run analysis with correct valid objects
    def run_analysis_for_range(s_dt, e_dt):
        index_res = []
        for idx in INDICES:
            res = analyze_ticker(idx, data, s_dt, e_dt)
            if res: index_res.append(res)
            
        sec_results = {}
        for sector, holdings in SECTORS.items():
            res = analyze_sector(sector, holdings, data, s_dt, e_dt)
            if res: sec_results[sector] = res
            
        if sec_results:
            report_text = generate_narrative_report(sec_results, index_res, s_dt, e_dt)
            return report_text
        return "No data found for this range."

    report = run_analysis_for_range(start_dt, end_dt)
    
    # Filename based on range for clarity if needed, or just standard
    with open("analysis_output.txt", "w", encoding='utf-8') as f:
        f.write(report)
    print("Report Generated.")

if __name__ == "__main__":
    main()
