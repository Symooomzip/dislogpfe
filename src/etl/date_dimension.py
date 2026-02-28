"""
Build the date dimension DataFrame for DimDate (DateKey = YYYYMMDD).
Matches StarSchema.DimDate columns.
"""
from datetime import datetime

import pandas as pd


def build_date_dimension(start_date: str | datetime, end_date: str | datetime) -> pd.DataFrame:
    """
    Create date dimension from start_date to end_date (inclusive).
    Columns: DateKey, FullDate, Year, Quarter, Month, Day, DayOfWeek, DayName, MonthName, IsWeekend.
    """
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date).date()
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date).date()
    if hasattr(start_date, "date"):
        start_date = start_date.date() if callable(getattr(start_date, "date", None)) else start_date
    if hasattr(end_date, "date"):
        end_date = end_date.date() if callable(getattr(end_date, "date", None)) else end_date
    if isinstance(start_date, pd.Timestamp):
        start_date = start_date.date()
    if isinstance(end_date, pd.Timestamp):
        end_date = end_date.date()

    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]

    rows = []
    for d in dates:
        d = d.date()
        rows.append({
            "DateKey": int(d.strftime("%Y%m%d")),
            "FullDate": d.isoformat(),
            "Year": d.year,
            "Quarter": (d.month - 1) // 3 + 1,
            "Month": d.month,
            "Day": d.day,
            "DayOfWeek": d.weekday() + 1,  # 1=Monday, 7=Sunday
            "DayName": day_names[d.weekday()],
            "MonthName": month_names[d.month - 1],
            "IsWeekend": 1 if d.weekday() >= 5 else 0,
        })
    return pd.DataFrame(rows)
