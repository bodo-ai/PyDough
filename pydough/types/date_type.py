"""
Definition of the PyDough type for dates.
"""

__all__ = ["DatetimeType"]


from .pydough_type import PyDoughType


class DatetimeType(PyDoughType):
    """
    The PyDough type representing dates with a year/month/day.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "DatetimeType()"

    @property
    def json_string(self) -> str:
        return "date"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        return DatetimeType() if type_string == "date" else None
