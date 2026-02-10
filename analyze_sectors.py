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

def filter_data_by_date(df, start_date_str=None, end_date_str=None):
    if df is None or df.empty: return df
    filtered = df.copy()
    is_tz_aware = filtered.index.tzinfo is not None
    timezone = pytz.timezone("America/New_York")
    
    if start_date_str:
        try:
            start_dt = pd.to_datetime(start_date_str)
            if is_tz_aware and start_dt.tzinfo is None:
                start_dt = timezone.localize(start_dt)
            filtered = filtered[filtered.index >= start_dt]
        except: pass

    if end_date_str:
        try:
            end_dt = pd.to_datetime(end_date_str)
            if is_tz_aware and end_dt.tzinfo is None:
                end_dt = timezone.localize(end_dt)
            filtered = filtered[filtered.index <= end_dt]
        except: pass
    return filtered

def analyze_last_day_shape(df):
    if df.empty: return 0, "N/A", 0
    last_date = df.index[-1].date()
    last_day_df = df[df.index.date == last_date]
    if last_day_df.empty: return 0, "N/A", 0
        
    open_p = last_day_df.iloc[0]['Open']
    close_p = last_day_df.iloc[-1]['Close']
    high_p = last_day_df['High'].max()
    low_p = last_day_df['Low'].min()
    
    move_pct = (close_p - open_p) / open_p * 100
    range_len = high_p - low_p
    if range_len == 0: return 0, "Doji", move_pct
    
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
        
    return score, desc, move_pct

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
    ret = (end_p - start_p) / start_p * 100
    score, desc, move = analyze_last_day_shape(df)
    
    grade, scenarios = generate_three_scenarios(ret, score, move)
    
    return {
        "Ticker": ticker,
        "Start": start_p,
        "End": end_p,
        "Return": ret,
        "LastScore": score,
        "LastDesc": desc,
        "LastMove": move,
        "Grade": grade,
        "Scenarios": scenarios
    }

def analyze_sector(sector_ticker, holdings, data, start_arg=None, end_arg=None):
    s_res = analyze_ticker(sector_ticker, data, start_arg, end_arg)
    if not s_res: return None
    
    stats = []
    for stock in holdings:
        st_res = analyze_ticker(stock, data, start_arg, end_arg)
        if not st_res: continue
        
        rel_trend = st_res['Return'] - s_res['Return']
        role = "NEUTRAL"
        reason = ""
        
        # Determine Role
        if rel_trend > 1.0:
            if st_res['LastScore'] >= 0:
                role = "ENGINE (牽引)"
                reason = f"トレンド牽引 (+{st_res['Return']:.1f}%)"
            else:
                role = "ENGINE (牽引)"
                reason = f"トレンドは強いが、直近で失速 ({st_res['LastDesc']})"
        elif rel_trend < -1.0:
            if st_res['LastScore'] > 0:
                role = "BRAKE (重石)"
                reason = f"出遅れだが、直近は買われている ({st_res['LastDesc']})"
            else:
                role = "BRAKE (重石)"
                reason = f"トレンドも直近も弱い ({st_res['LastDesc']})"
        else:
            if s_res['LastMove'] < -0.3 and st_res['LastMove'] > 0.3:
                role = "ENGINE (牽引)"
                reason = "セクター下落の中で逆行高"
            elif s_res['LastMove'] > 0.3 and st_res['LastMove'] < -0.3:
                role = "BRAKE (重石)"
                reason = "セクター上昇についていけず失速"
                
        st_res['Role'] = role
        st_res['Reason'] = reason
        stats.append(st_res)

    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        stats_df = stats_df.sort_values("Return", ascending=False)
    
    return {
        "sector": sector_ticker,
        "name": SECTOR_NAMES.get(sector_ticker, sector_ticker),
        "return": s_res['Return'],
        "start_p": s_res['Start'],
        "end_p": s_res['End'],
        "last_desc": s_res['LastDesc'],
        "last_move": s_res['LastMove'],
        "grade": s_res['Grade'],
        "scenarios": s_res['Scenarios'],
        "stats": stats_df
    }

def generate_narrative_report(results, index_results, start_dt, end_dt):
    report = []
    report.append("【天才投資家レポート】")
    report.append(f"分析期間: {start_dt} 〜 {end_dt}\n")
    
    # 1. Indices (Detailed)
    report.append("### ① 全体観 (Indices)")
    for idx_res in index_results:
        idx = idx_res['Ticker']
        name = SECTOR_NAMES.get(idx, idx)
        
        report.append(f"**{name} ({idx})**: {idx_res['Grade']}")
        report.append(f"  Price: {idx_res['Start']:.2f} -> {idx_res['End']:.2f} ({idx_res['Return']:+.2f}%)")
        report.append(f"  直近: {idx_res['LastDesc']} ({idx_res['LastMove']:+.1f}%)")
        
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
    report.append("### ② マクロ結論: 資金流動")
    report.append(f"資金は**「{loser['name']}」から「{winner['name']}」へ**シフトしています。")
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
        
        # Scenarios for Sector (Removed at user request)
        # sc = res['scenarios']
        # report.append("**想定シナリオ**:")
        # report.append(f"(普): {sc['Avg']}")
        # report.append(f"(良): {sc['Good']}")
        # report.append(f"(悪): {sc['Bad']}")
        
        report.append(f"**Price**: ${res['start_p']:.2f} -> ${res['end_p']:.2f} ({res['return']:+.2f}%)")
        report.append(f"**直近**: {res['last_desc']}")
        
        if not engines.empty:
            report.append("🔥 **Engine (牽引)**:")
            for _, row in engines.iterrows():
                report.append(f"- {row['Ticker']}: {row['Start']:.2f}->{row['End']:.2f} ({row['Return']:+.1f}%): {row['Reason']}")
        
        if not brakes.empty:
            report.append("🧊 **Brake (重石)**:")
            for _, row in brakes.iterrows():
                report.append(f"- {row['Ticker']}: {row['Start']:.2f}->{row['End']:.2f} ({row['Return']:+.1f}%): {row['Reason']}")
        
        report.append("\n" + "-"*20 + "\n")

    return "\n".join(report)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str)
    parser.add_argument('--end', type=str)
    parser.add_argument('--days', type=int, default=14)
    args = parser.parse_args()
    
    data = fetch_data()
    if data is None: return

    end_dt = datetime.now()
    if args.end: end_dt = pd.to_datetime(args.end)
    start_dt = end_dt - timedelta(days=args.days)
    if args.start: start_dt = pd.to_datetime(args.start)
        
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")
    
    print(f"Analyzing {start_str} to {end_str}...")
    
    index_results = []
    for idx in INDICES:
        res = analyze_ticker(idx, data, start_str, end_str)
        if res: index_results.append(res)

    results = {}
    for sector, holdings in SECTORS.items():
        res = analyze_sector(sector, holdings, data, start_str, end_str)
        if res: results[sector] = res

    if results:
        report = generate_narrative_report(results, index_results, start_str, end_str)
        with open("analysis_output.txt", "w", encoding='utf-8') as f:
            f.write(report)
        print("Report Generated.")

if __name__ == "__main__":
    main()
