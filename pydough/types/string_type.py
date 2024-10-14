from .pydough_type import PyDoughType


class StringType(PyDoughType):
    def __init__(self):
        pass

    def __repr__(self):
        return "StringType()"

    def as_json_string(self):
        return "string"
