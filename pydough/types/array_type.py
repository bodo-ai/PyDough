from .pydough_type import PyDoughType
from .errors import PyDoughTypeException


class ArrayType(PyDoughType):
    def __init__(self, elem_type: PyDoughType):
        if not isinstance(elem_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid component type for ArrayType. Expected a PyDoughType, received: {repr(elem_type)}"
            )
        self.elem_type = elem_type

    def __repr__(self):
        return f"ArrayType({repr(self.elem_type)})"

    def as_json_string(self):
        return f"array[{self.elem_type.as_json_string()}]"
