"""Filter yfinance intraday bars to US regular trading hours (9:30-15:45 NY time).

The last bar of a normal US session is the 15-minute bar starting at 15:45 NY,
which is JST 04:45 during DST (EDT) / JST 05:45 during EST.
pytz handles the DST/EST switch automatically from each bar's timestamp.
"""
import pytz

NY = pytz.timezone("America/New_York")


def filter_to_us_regular_hours(df):
    if df is None or df.empty or df.index.tzinfo is None:
        return df
    ny_times = df.index.tz_convert(NY)
    minutes = ny_times.hour * 60 + ny_times.minute
    mask = (minutes >= 9 * 60 + 30) & (minutes <= 15 * 60 + 45)
    return df[mask]
