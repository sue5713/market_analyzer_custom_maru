"""Wrapper that computes the RF analysis baseline by weekday, applies the US
market-hours filter (9:30-15:45 NY time) and close-to-close alignment, then
runs analyze_sectors.main() in-process.

Baseline rules (reference = latest trading day):
- Mon-Thu: baseline = previous week's Friday close
- Fri:     baseline = last day of previous month close

Close-to-close alignment (close vs close, not open vs close):
- baseline-day's last 15:45 NY bar -> that bar's Close = baseline close
- A synthetic bar with OHLC all equal to baseline close is prepended
- baseline-day's actual bars are dropped
- Downstream: df.iloc[0]['Open'] now equals the baseline close, so the
  existing (end_close - start_open) / start_open formula in analyze_sectors
  produces a close-to-close return.
"""
import argparse
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pytz


def compute_auto_baseline(market_type: str = "US"):
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)

    if market_type.upper() == "US":
        ref_date = (now_jst - timedelta(days=1)).date()
    else:
        ref_date = now_jst.date()

    while ref_date.weekday() >= 5:
        ref_date -= timedelta(days=1)

    wd = ref_date.weekday()
    if wd == 4:
        first_of_month = ref_date.replace(day=1)
        baseline = first_of_month - timedelta(days=1)
        while baseline.weekday() >= 5:
            baseline -= timedelta(days=1)
        label = "前月末終値基点 (Previous Month Close)"
    else:
        baseline = ref_date - timedelta(days=wd + 3)
        label = "前週末終値基点 (Previous Week Close)"

    return baseline, ref_date, label


def _apply_close_to_close(df):
    if df is None or df.empty or len(df) < 2:
        return df
    first_date = df.index[0].date()
    first_day = df[df.index.date == first_date]
    rest = df[df.index.date > first_date]
    if first_day.empty or rest.empty:
        return df
    baseline_close = first_day.iloc[-1]['Close']
    baseline_ts = first_day.index[-1]
    row = {}
    for col in df.columns:
        if col in ('Open', 'High', 'Low', 'Close'):
            row[col] = [baseline_close]
        else:
            row[col] = [0]
    synthetic = pd.DataFrame(row, index=[baseline_ts])
    return pd.concat([synthetic, rest])


def install_us_filter_and_baseline():
    import analyze_sectors
    from market_hours_filter import filter_to_us_regular_hours

    original = analyze_sectors.filter_data_by_date

    def patched(df, start=None, end=None):
        df = filter_to_us_regular_hours(original(df, start, end))
        return _apply_close_to_close(df)

    analyze_sectors.filter_data_by_date = patched


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", default=os.environ.get("MARKET_TYPE", "US"))
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args, _ = parser.parse_known_args()

    if args.start or args.end:
        new_argv = ["analyze_sectors.py"]
        if args.start:
            new_argv += ["--start", args.start]
        if args.end:
            new_argv += ["--end", args.end]
        print(f"[MANUAL] Forwarding to analyze_sectors: {' '.join(new_argv[1:])}")
    else:
        baseline, ref, label = compute_auto_baseline(args.market)
        start_str = baseline.strftime("%Y-%m-%d")
        end_str = ref.strftime("%Y-%m-%d")
        print(f"[AUTO] {label}")
        print(f"[AUTO] market={args.market} baseline={start_str} ref={end_str} weekday={ref.weekday()}")
        new_argv = ["analyze_sectors.py", "--start", start_str, "--end", end_str]

    if args.market.upper() == "US":
        install_us_filter_and_baseline()
        print("[FILTER] US 9:30-15:45 NY + close-to-close baseline installed")

    sys.argv = new_argv
    import analyze_sectors
    analyze_sectors.main()


if __name__ == "__main__":
    main()
