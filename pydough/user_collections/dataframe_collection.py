"""
A user-defined collection of pandas Dataframes.
Usage:
`pydough.dataframe_collection(name, dataframe)`

This module defines a collection that generated from a given pandas Dataframe.
The user must specify the name of the collection and the
name of the dataframe that will hold the values.

The pandas dataframe must have at least one column with one item. The accepted
types are: NumericType, BooleanType, DatetimeType, StringType and UnknownType.
Mixed types columsn are not allowed.
"""

import pandas as pd
import pyarrow as pa

from pydough.types.boolean_type import BooleanType
from pydough.types.datetime_type import DatetimeType
from pydough.types.numeric_type import NumericType
from pydough.types.pydough_type import PyDoughType
from pydough.types.string_type import StringType
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
        if len(dataframe.columns) == 0:
            raise ValueError(
                "Dataframe is empty. Must have at least one column and one value."
            )

        for col in list(dataframe.columns):
            types_list = sorted(
                {type(v) for v in dataframe[col].dropna()},
                key=lambda t: t.__name__,
            )
            if len(types_list) > 1:
                raise TypeError(
                    f"Mixed types in column '{col}': {types_list}. All values in a column must be of the same type."
                )
            elif len(dataframe[col]) == 0:
                raise TypeError(
                    f"Column '{col}' is empty. All columns must have at least one value."
                )

        pyd_types: list[PyDoughType] = []

        for field in pa.Schema.from_pandas(dataframe):
            pyd_types.append(
                DataframeGeneratedCollection.match_pyarrow_pydough_types(field.type)
            )

        return pyd_types

    @staticmethod
    def match_pyarrow_pydough_types(field_type: pa.DataType) -> PyDoughType:
        match field_type:
            case _ if pa.types.is_null(field_type):
                return UnknownType()

            case _ if pa.types.is_boolean(field_type):
                return BooleanType()

            case _ if (
                pa.types.is_integer(field_type)
                or pa.types.is_floating(field_type)
                or pa.types.is_decimal(field_type)
            ):
                return NumericType()

            case _ if pa.types.is_string(field_type) or pa.types.is_large_string(
                field_type
            ):
                return StringType()

            case _ if pa.types.is_binary(field_type) or pa.types.is_large_binary(
                field_type
            ):
                return StringType()

            case _ if (
                pa.types.is_date(field_type)
                or pa.types.is_timestamp(field_type)
                or pa.types.is_time(field_type)
                or pa.types.is_duration(field_type)
            ):
                return DatetimeType()

            # Unsupported datatypes
            case _ if pa.types.is_list(field_type) or pa.types.is_large_list(
                field_type
            ):
                raise ValueError("Arrays are not supported for dataframe collections")

            case _ if pa.types.is_struct(field_type):
                raise ValueError("Structs are not supported for dataframe collections")

            case _ if pa.types.is_dictionary(field_type):
                raise ValueError(
                    "Dictionaries are not supported for dataframe collections"
                )
            case _:
                return UnknownType()
