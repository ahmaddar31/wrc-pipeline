from datetime import date, datetime, timedelta
from typing import Iterator, Tuple


def parse_cli_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def to_site_date(value: date) -> str:
    """Format as DD/MM/YYYY with zero-padding, as required by the WRC search API."""
    return value.strftime("%d/%m/%Y")


def month_ranges(start_date: date, end_date: date) -> Iterator[Tuple[date, date]]:
    current = date(start_date.year, start_date.month, 1)

    while current <= end_date:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)

        month_end = next_month - timedelta(days=1)
        part_start = max(start_date, current)
        part_end = min(end_date, month_end)

        yield part_start, part_end
        current = next_month


def week_ranges(start_date: date, end_date: date) -> Iterator[Tuple[date, date]]:
    """Yield (week_start, week_end) pairs covering start_date..end_date.

    Weeks are calendar weeks starting on Monday.  The first and last windows
    are clipped to start_date / end_date so partial weeks at the boundaries
    are included without over-fetching.
    """
    # Rewind to the Monday of the week containing start_date
    current = start_date - timedelta(days=start_date.weekday())

    while current <= end_date:
        week_end = current + timedelta(days=6)
        part_start = max(start_date, current)
        part_end = min(end_date, week_end)

        yield part_start, part_end
        current += timedelta(weeks=1)