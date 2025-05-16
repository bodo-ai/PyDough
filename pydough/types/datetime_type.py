"""
Definition of the PyDough type for the datetime type.
"""

__all__ = ["DatetimeType"]


from .pydough_type import PyDoughType


class DatetimeType(PyDoughType):
    """
    The PyDough type for date/timestamp values.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "DatetimeType()"

    @property
    def json_string(self) -> str:
        return "datetime"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        return DatetimeType() if type_string == "datetime" else None
