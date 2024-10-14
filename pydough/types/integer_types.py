from .pydough_type import PyDoughType


class IntegerType(PyDoughType):
    def __repr__(self):
        return f"Integer{self.bit_width}Type()"

    def as_json_string(self):
        return f"int{self.bit_width}"


class Int8Type(IntegerType):
    def __init__(self):
        self.bit_width = 8


class Int16Type(IntegerType):
    def __init__(self):
        self.bit_width = 16


class Int32Type(IntegerType):
    def __init__(self):
        self.bit_width = 32


class Int64Type(IntegerType):
    def __init__(self):
        self.bit_width = 64
