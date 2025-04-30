"""
Unit tests the PyDough types module.
"""

import pytest

# Used so eval() has access to all the type classes
from pydough.types import *  # noqa
from pydough.types import parse_type_from_string


@pytest.mark.parametrize(
    "type_string, repr_string",
    [
        pytest.param("numeric", "NumericType()", id="numeric"),
        pytest.param("bool", "BooleanType()", id="bool"),
        pytest.param("datetime", "DatetimeType()", id="date"),
        pytest.param("string", "StringType()", id="string"),
        pytest.param("array[numeric]", "ArrayType(NumericType())", id="array-numeric"),
        pytest.param("array[string]", "ArrayType(StringType())", id="array-string"),
        pytest.param(
            "array[array[datetime]]",
            "ArrayType(ArrayType(DatetimeType()))",
            id="array-array-datetime",
        ),
        pytest.param(
            "array[array[array[float64]]]",
            "ArrayType(ArrayType(ArrayType(NumericType())))",
            id="array-array-array-float64",
        ),
        pytest.param(
            "map[string,numeric]",
            "MapType(StringType(),NumericType())",
            id="map-string-numeric",
        ),
        pytest.param(
            "map[string,datetime]",
            "MapType(StringType(),DatetimeType())",
            id="map-string-datetime",
        ),
        pytest.param(
            "map[string,array[map[string,datetime]]]",
            "MapType(StringType(),ArrayType(MapType(StringType(),DatetimeType())))",
            id="map-string-array-map-string-date",
        ),
        pytest.param(
            "map[map[map[string,numeric],map[numeric,string]],map[map[string,datetime],date]]",
            "MapType(MapType(MapType(StringType(),NumericType()),MapType(NumericType(),StringType())),MapType(MapType(StringType(),TimeType(0)),DatetimeType()))",
            id="map-multi_nested_maps",
        ),
        pytest.param(
            "struct[name:string]",
            "StructType([('name', StringType())])",
            id="struct-single_field",
        ),
        pytest.param(
            "struct[x:float64,y:float64]",
            "StructType([('x', NumericType()), ('y', NumericType())])",
            id="struct-cartesian_points",
        ),
        pytest.param(
            "struct[first_name:string,last_name:string,birthday:date]",
            "StructType([('first_name', StringType()), ('last_name', StringType()), ('birthday', DatetimeType())])",
            id="struct-person",
        ),
        pytest.param(
            "struct[spouse:struct[first_name:string,last_name:string,birthday:date],children:array[struct[first_name:string,last_name:string,birthday:date]]]",
            "StructType([('spouse', StructType([('first_name', StringType()), ('last_name', StringType()), ('birthday', DatetimeType())])), ('children', ArrayType(StructType([('first_name', StringType()), ('last_name', StringType()), ('birthday', DatetimeType())])))])",
            id="struct-parent",
        ),
        pytest.param(
            "map[struct[x:numeric,y:numeric],struct[a:numeric,b:map[string,struct[c:numeric,d:numeric,e:numeric]]]]",
            "MapType(StructType([('x', NumericType()), ('y', NumericType())]),StructType([('a', NumericType()), ('b', MapType(StringType(),StructType([('c', NumericType()), ('d', NumericType()), ('e', NumericType())])))]))",
            id="multi_nested",
        ),
        pytest.param(
            "unknown",
            "UnknownType()",
            id="unknown",
        ),
        pytest.param(
            "array[unknown]",
            "ArrayType(UnknownType())",
            id="array-unknown",
        ),
        pytest.param(
            "map[string,unknown]",
            "MapType(StringType(),UnknownType())",
            id="map-unknown",
        ),
    ],
)
def test_valid_parsing_and_unparsing(type_string: str, repr_string: str) -> None:
    """
    Verify that each type can be correctly parsed from its JSON string form into
    the correct type object, and unparsed into either its correct Python repr form
    or back into its JSON string from. Also verifies that invoking eval
    on the repr string creates the same type object.
    """
    pydough_type = parse_type_from_string(type_string)
    assert repr(pydough_type) == repr_string, (
        "parsed type does not match expected repr() string"
    )
    assert pydough_type.json_string == type_string, (
        "parsed type does not unparse back to the same JSON string"
    )
    eval_type = eval(repr_string)
    assert eval_type == pydough_type, "type is not equal to its evaluated repr() string"
    assert repr(eval_type) == repr_string, (
        "mismatch between repr string and the repr() of the type created by evaluating the repr() string"
    )
    assert eval_type.json_string == type_string, (
        "evaluated repr() string does not unparse back to the same JSON string"
    )
