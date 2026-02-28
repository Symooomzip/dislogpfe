"""
Build and return the date dimension DataFrame for a given date range.
"""
import pandas as pd
from datetime import datetime


def build_date_dimension(start_date: str | datetime, end_date: str | datetime) -> pd.DataFrame:
    """
    Create a date dimension table from start_date to end_date (inclusive).
    Columns: date_key, full_date, year, quarter, month, month_name, week_of_year,
             day_of_month, day_of_week, day_name, is_weekend.
    """
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date).date()
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date).date()
    if isinstance(start_date, pd.Timestamp):
        start_date = start_date.date()
    if isinstance(end_date, pd.Timestamp):
        end_date = end_date.date()

    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    rows = []
    for d in dates:
        d = d.date()
        rows.append({
            "date_key": int(d.strftime("%Y%m%d")),
            "full_date": d.isoformat(),
            "year": d.year,
            "quarter": (d.month - 1) // 3 + 1,
            "month": d.month,
            "month_name": month_names[d.month - 1],
            "week_of_year": d.isocalendar()[1],
            "day_of_month": d.day,
            "day_of_week": d.weekday() + 1,  # 1=Monday, 7=Sunday
            "day_name": day_names[d.weekday()],
            "is_weekend": 1 if d.weekday() >= 5 else 0,
        })
    return pd.DataFrame(rows)
