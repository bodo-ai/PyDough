from .pydough_type import PyDoughType


class DateType(PyDoughType):
    def __init__(self):
        pass

    def __repr__(self):
        return "DateType()"

    def as_json_string(self):
        return "date"
