from typing import NamedTuple, Optional
from datetime import datetime, date

class Customers(NamedTuple):
    """Structure for customers data"""
    id: int
    name: str
    last_tname: str
    genre: str #Optional[str]
    birth_date: date
