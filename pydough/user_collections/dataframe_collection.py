"""
TODO
"""

import pandas as pd
import pyarrow as pa

from pydough.types.array_type import ArrayType
from pydough.types.boolean_type import BooleanType
from pydough.types.datetime_type import DatetimeType
from pydough.types.map_type import MapType
from pydough.types.numeric_type import NumericType
from pydough.types.pydough_type import PyDoughType
from pydough.types.string_type import StringType
from pydough.types.struct_type import StructType
from pydough.types.unknown_type import UnknownType
from pydough.user_collections.user_collections import PyDoughUserGeneratedCollection

__all__ = ["DataframeGeneratedCollection"]


class DataframeGeneratedCollection(PyDoughUserGeneratedCollection):
    """Dataframe base collection"""

    def __init__(self, name: str, dataframe: pd.DataFrame) -> None:
        super().__init__(
            name=name,
            columns=list(dataframe.columns),
            types=self.get_dataframe_types(dataframe),
        )
        self._dataframe = dataframe

    @property
    def types(self) -> list[PyDoughType]:
        """Return a list containing the types of the dataframe"""
        return self._types

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return the dataframe"""
        return self._dataframe

    @property
    def column_names_and_types(self) -> list[tuple[str, PyDoughType]]:
        assert len(self.columns) == len(self.types)
        return [(self.columns[i], self.types[i]) for i in range(len(self.columns))]

    @property
    def unique_column_names(self) -> list[str]:
        return list(dict.fromkeys(self.columns))

    def __len__(self) -> int:
        return len(self._dataframe)

    def is_singular(self) -> bool:
        """Returns True if the collection is guaranteed to contain at most one row."""
        return len(self) <= 1

    def always_exists(self) -> bool:
        """Check if the range collection is always non-empty."""
        return len(self) > 0

    def to_string(self) -> str:
        """Return a string representation of the dataframe collection."""
        return (
            f"DataframeCollection("
            f"name={self.name!r}, "
            f"shape={self.dataframe.shape}, "
            f"columns={list(self.dataframe.columns)!r}"
            f")"
        )

    def equals(self, other) -> bool:
        return (
            isinstance(other, DataframeGeneratedCollection)
            and self.name == other.name
            and self.columns == other.columns
            and self.dataframe.equals(other.dataframe)
        )

    @staticmethod
    def get_dataframe_types(dataframe: pd.DataFrame) -> list[PyDoughType]:
        """
        Verify that each column has homogeneos type and gets its type
        and convert it to PyDoughType.
        """
        if len({type(v) for v in dataframe.dropna()}) > 1:
            raise TypeError("Mixed types columns are not allowed")

        pyd_types: list[PyDoughType] = []

        for field in pa.Schema.from_pandas(dataframe):
            pyd_types.append(
                DataframeGeneratedCollection.match_pyarrow_pydough_types(field)
            )

        return pyd_types

    @staticmethod
    def match_pyarrow_pydough_types(field: pa.Field) -> PyDoughType:
        match field.type:
            case pa.NullType():
                return UnknownType()
            case pa.BooleanType():
                return BooleanType()
            case (
                pa.Int8Type()
                | pa.Int16Type()
                | pa.Int32Type()
                | pa.Int64Type()
                | pa.UInt8Type()
                | pa.UInt16Type()
                | pa.UInt32Type()
                | pa.UInt64Type()
            ):
                return NumericType()
            case pa.Float16Type() | pa.Float32Type() | pa.Float64Type():
                return NumericType()
            case pa.Decimal128Type() | pa.Decimal256Type():
                return NumericType()
            case pa.StringType() | pa.LargeStringType():
                return StringType()
            case pa.BinaryType() | pa.LargeBinaryType():
                return StringType()
            case (
                pa.Date32Type()
                | pa.Date64Type()
                | pa.TimestampType()
                | pa.Time32Type()
                | pa.Time64Type()
                | pa.DurationType()
            ):
                return DatetimeType()
            case pa.ListType() | pa.LargeListType():
                return ArrayType(
                    DataframeGeneratedCollection.match_pyarrow_pydough_types(
                        field.type.value_type
                    )
                )
            case pa.StructType():
                return StructType(
                    [
                        (
                            field[i].name,
                            DataframeGeneratedCollection.match_pyarrow_pydough_types(
                                field[i].type
                            ),
                        )
                        for i in range(len(field))
                    ]
                )
            case pa.DictionaryType():
                return MapType(
                    DataframeGeneratedCollection.match_pyarrow_pydough_types(
                        field.index_type
                    ),
                    DataframeGeneratedCollection.match_pyarrow_pydough_types(
                        field.value_type
                    ),
                )
            case _:
                return UnknownType()
