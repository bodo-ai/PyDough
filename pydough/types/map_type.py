from .pydough_type import PyDoughType
from .errors import PyDoughTypeException


class MapType(PyDoughType):
    def __init__(self, key_type: PyDoughType, val_type: PyDoughType):
        if not isinstance(key_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid key type for ArrayType. Expected a PyDoughType, received: {repr(key_type)}"
            )
        if not isinstance(key_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid value type for ArrayType. Expected a PyDoughType, received: {repr(val_type)}"
            )
        self.key_type = key_type
        self.val_type = val_type

    def __repr__(self):
        return f"MapType({repr(self.key_type)},{repr(self.val_type)})"

    def as_json_string(self):
        return f"map[{self.key_type.as_json_string()},{self.val_type.as_json_string()}]"
