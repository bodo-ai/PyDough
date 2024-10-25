"""
TODO: add file-level docstring.
"""

from pydough.types import parse_type_from_string

# Used so eval() has access to all the type classes
from pydough.types import *  # noqa
import pytest


@pytest.mark.parametrize(
    "type_string, repr_string",
    [
        pytest.param("int8", "Int8Type()", id="int8"),
        pytest.param("int16", "Int16Type()", id="int16"),
        pytest.param("int32", "Int32Type()", id="int32"),
        pytest.param("int64", "Int64Type()", id="int64"),
        pytest.param("float32", "Float32Type()", id="float32"),
        pytest.param("float64", "Float64Type()", id="float64"),
        pytest.param("bool", "BooleanType()", id="bool"),
        pytest.param("date", "DateType()", id="date"),
        pytest.param("string", "StringType()", id="string"),
        pytest.param("binary", "BinaryType()", id="binary"),
        pytest.param("time[0]", "TimeType(0)", id="time-0"),
        pytest.param("time[3]", "TimeType(3)", id="time-3"),
        pytest.param("time[6]", "TimeType(6)", id="time-6"),
        pytest.param("time[9]", "TimeType(9)", id="time-9"),
        pytest.param("timestamp[0]", "TimestampType(0,None)", id="timestamp-0-no_tz"),
        pytest.param(
            "timestamp[3,America/Los_Angeles]",
            "TimestampType(3,'America/Los_Angeles')",
            id="timestamp-3-los_angeles",
        ),
        pytest.param("timestamp[6]", "TimestampType(6,None)", id="timestamp-6-no_tz"),
        pytest.param(
            "timestamp[9,Europe/Berlin]",
            "TimestampType(9,'Europe/Berlin')",
            id="timestamp-9-berlin",
        ),
        pytest.param("decimal[1,0]", "DecimalType(1,0)", id="decimal-1-0"),
        pytest.param("decimal[2,1]", "DecimalType(2,1)", id="decimal-2-1"),
        pytest.param("decimal[4,0]", "DecimalType(4,0)", id="decimal-4-0"),
        pytest.param("decimal[8,7]", "DecimalType(8,7)", id="decimal-8-7"),
        pytest.param("decimal[16,0]", "DecimalType(16,0)", id="decimal-16-0"),
        pytest.param("decimal[32,16]", "DecimalType(32,16)", id="decimal-32-16"),
        pytest.param("decimal[38,0]", "DecimalType(38,0)", id="decimal-38-0"),
        pytest.param("decimal[38,18]", "DecimalType(38,18)", id="decimal-38-18"),
        pytest.param("array[int8]", "ArrayType(Int8Type())", id="array-int8"),
        pytest.param("array[string]", "ArrayType(StringType())", id="array-string"),
        pytest.param(
            "array[timestamp[6,US/Eastern]]",
            "ArrayType(TimestampType(6,'US/Eastern'))",
            id="array-timestamp-3-est",
        ),
        pytest.param(
            "array[array[date]]",
            "ArrayType(ArrayType(DateType()))",
            id="array-array-date",
        ),
        pytest.param(
            "array[array[array[float64]]]",
            "ArrayType(ArrayType(ArrayType(Float64Type())))",
            id="array-array-array-float64",
        ),
        pytest.param(
            "map[string,int32]",
            "MapType(StringType(),Int32Type())",
            id="map-string-int32",
        ),
        pytest.param(
            "map[string,time[0]]",
            "MapType(StringType(),TimeType(0))",
            id="map-string-time-0",
        ),
        pytest.param(
            "map[string,array[map[string,date]]]",
            "MapType(StringType(),ArrayType(MapType(StringType(),DateType())))",
            id="map-string-array-map-string-date",
        ),
        pytest.param(
            "map[map[map[string,int32],map[int64,string]],map[map[string,time[0]],date]]",
            "MapType(MapType(MapType(StringType(),Int32Type()),MapType(Int64Type(),StringType())),MapType(MapType(StringType(),TimeType(0)),DateType()))",
            id="map-multi_nested_maps",
        ),
        pytest.param(
            "struct[name:string]",
            "StructType([('name', StringType())])",
            id="struct-single_field",
        ),
        pytest.param(
            "struct[x:float64,y:float64]",
            "StructType([('x', Float64Type()), ('y', Float64Type())])",
            id="struct-cartesian_points",
        ),
        pytest.param(
            "struct[first_name:string,last_name:string,birthday:date]",
            "StructType([('first_name', StringType()), ('last_name', StringType()), ('birthday', DateType())])",
            id="struct-person",
        ),
        pytest.param(
            "struct[spouse:struct[first_name:string,last_name:string,birthday:date],children:array[struct[first_name:string,last_name:string,birthday:date]]]",
            "StructType([('spouse', StructType([('first_name', StringType()), ('last_name', StringType()), ('birthday', DateType())])), ('children', ArrayType(StructType([('first_name', StringType()), ('last_name', StringType()), ('birthday', DateType())])))])",
            id="struct-parent",
        ),
        pytest.param(
            "map[struct[x:int8,y:int8],struct[a:int8,b:map[string,struct[c:int8,d:int8,e:int8]]]]",
            "MapType(StructType([('x', Int8Type()), ('y', Int8Type())]),StructType([('a', Int8Type()), ('b', MapType(StringType(),StructType([('c', Int8Type()), ('d', Int8Type()), ('e', Int8Type())])))]))",
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
def test_valid_parsing_and_unparsing(type_string, repr_string):
    """
    Verify that each type can be correctly parsed from its JSON string form into
    the correct type object, and unparsed into either its correct Python repr form
    or back into its JSON string from. Also verifies that invoking eval
    on the repr string creates the same type object.
    """
    pydough_type = parse_type_from_string(type_string)
    assert (
        repr(pydough_type) == repr_string
    ), "parsed type does not match expected repr() string"
    assert (
        pydough_type.json_string == type_string
    ), "parsed type does not unparse back to the same JSON string"
    eval_type = eval(repr_string)
    assert eval_type == pydough_type, "type is not equal to its evaluated repr() string"
    assert (
        repr(eval_type) == repr_string
    ), "mismatch between repr string and the repr() of the type created by evaluating the repr() string"
    assert (
        eval_type.json_string == type_string
    ), "evaluated repr() string does not unparse back to the same JSON string"
