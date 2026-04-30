from __future__ import annotations

from datetime import datetime


def normalize_session_name(value_or_hour) -> str:
    if isinstance(value_or_hour, int):
        hour = value_or_hour
    else:
        s = str(value_or_hour or '').strip().lower()
        mapping = {
            'asia':'Asia','tokyo':'Asia',
            'london':'London',
            'new_york':'New_York','new york':'New_York','ny':'New_York',
            'london/ny_overlap':'London/NY_overlap','london_ny':'London/NY_overlap','london-ny':'London/NY_overlap',
        }
        if s in mapping:
            return mapping[s]
        if 'overlap' in s or ('london' in s and 'ny' in s):
            return 'London/NY_overlap'
        if s.isdigit():
            hour = int(s)
        else:
            hour = datetime.utcnow().hour
    if hour < 7:
        return 'Asia'
    if hour < 13:
        return 'London'
    if hour < 17:
        return 'London/NY_overlap'
    return 'New_York'
