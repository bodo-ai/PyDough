# PyDough Types

This subdirectory of the PyDough directory deals with the definitions of data types that are propagated throughout PyDough to help identify what each data column is.

The types module provides functionality to define and manage various data types used within PyDough.

## Available APIs

### [pydough_type.py](pydough_type.py)

- `PyDoughType`: The abstract base class describing all PyDough types. Each implementation class must define the following:
    - A constructor
    - An eval-friendly `__repr__` such that two PyDough types with the same repr must be the same type.
    - `json_string` property
    - `parse_from_string` static method

### [numeric_type.py](numeric_type.py)

- `NumericType`: The PyDough type for integers, floats and decimals, regardless of scale/precision.

### [boolean_type.py](boolean_type.py)

- `BooleanType`: The PyDough type representing boolean values.

### [string_type.py](string_type.py)

- `StringType`: The PyDough type representing strings and other bytes-like data.

### [datetime_type.py](datetime_type.py)

- `DatetimeType`: The PyDough type representing date/timestamp values, regardless of precision/time zone.

### [array_type.py](array_type.py)

- `ArrayType`: The PyDough type representing an array of data. The type has another PyDough type representing the type of its elements.

### [map_type.py](map_type.py)

- `MapType`: The PyDough type representing a map of key-value pairs. The type contains two additional PyDough types representing the types of the keys versus the values.

### [struct_type.py](struct_type.py)

- `StructType`: The PyDough type representing a collection of named fields. The fields are represented as a list of tuples of field names and their PyDough types.

### [unknown_type.py](unknown_type.py)

- `UnknownType`: The PyDough type representing an unknown type.

### [parse_types.py](parse_types.py)

- `parse_type_from_string`: Converts a string from a JSON file representing a PyDough type and converts it to that PyDough type.

### [errors.py](errors.py)

- `PyDoughTypeException`: Exception raised when there is an error relating to PyDough types, such as malformed inputs to a parameterized type or a string that cannot be parsed into a type.

## Key Interactions

The types module provides a comprehensive set of data types that can be used to define the schema of data columns in PyDough. These types can be parsed from strings, serialized to JSON, and used to ensure type safety and correctness in PyDough operations.

## Usage

To use the types module, you can import the necessary classes and call them with the appropriate arguments. For example:

```python
from pydough.types import NumericType, StringType, StructType, parse_type_from_string

# Create an integer type
nt = NumericType()

# Create a string type
st = StringType()

# Create a struct type with fields
struct_type = StructType([("id", nt), ("name", st)])

# Serialize the struct type to a JSON string
json_string = struct_type.json_string

# Parse the struct type from a JSON string
parsed_type = StructType.parse_from_string(json_string)

# Parse a map type from a json string (the keys are strings and the values are
# arrays of numeric values)
map_str_arr_numeric = parse_type_from_string("map[string,array[numeric]]")
```
