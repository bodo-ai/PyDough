"""
A user-defined collection of Pandas Dataframes.
Usage:
`pydough.dataframe_collection(name, dataframe)`

This module defines a collection that is generated from a given Pandas Dataframe.
The user must specify the name of the collection and the
name of the dataframe that has the values.

The Pandas Dataframe must have at least one column with one value.
The supported PyDough types are:
- NumericType
- BooleanType
- DatetimeType
- StringType
- UnknownType

Pandas supported datatypes:
- null
- string
- int64
- float64
- decimal
- bool
- datetime64

Mixed types columns are NOT supported.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.types as pa_types

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

    def __init__(
        self,
        name: str,
        dataframe: pd.DataFrame,
        unique_column_names: list[str | list[str]],
    ) -> None:
        super().__init__(
            name=name,
            columns=list(dataframe.columns),
            types=self.get_dataframe_types(dataframe),
        )
        self._dataframe = dataframe
        self._unique_column_names = unique_column_names

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
        return list(zip(self.columns, self.types))

    @property
    def unique_column_names(self) -> list[str | list[str]]:
        return self._unique_column_names

    def __len__(self) -> int:
        return len(self._dataframe)

    def is_singular(self) -> bool:
        """Returns True if the collection is guaranteed to contain at most one row."""
        return len(self) <= 1

    def always_exists(self) -> bool:
        """Check if the dataframe collection is always non-empty."""
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
            and self.unique_column_names == other.unique_column_names
            and self.dataframe.equals(other.dataframe)
        )

    @staticmethod
    def get_dataframe_types(dataframe: pd.DataFrame) -> list[PyDoughType]:
        """
        Validate the given dataframe and collect the PyDough types matching each
        column respectively.

        Validations:
            - The dataframe must have at least one column.
            - The dataframe must have at least one value per column.
            - The dataframe must have one datatype per column.

        Args:
            `dataframe`: The Pandas Dataframe to get the PyDouhg types from.

        Returns:
            List of PyDough types matching each column
        """
        if len(dataframe.columns) == 0:
            raise ValueError(
                "DataFrame is empty. Must have at least one non-empty column."
            )

        if len(dataframe) == 0:
            raise ValueError("DataFrame has no rows. Must have at least one row.")

        pyd_types: list[PyDoughType] = []

        for col in dataframe.columns:
            try:
                field: pa.Field = pa.Schema.from_pandas(dataframe[[col]])
                pyd_types.append(
                    DataframeGeneratedCollection.match_pyarrow_pydough_types(
                        field.types[0], col
                    )
                )

            except pa.ArrowInvalid as e:
                raise ValueError(
                    f"Failed to infer a consistent type for column '{col}'. "
                    f"Arrow error: {e}"
                )

        return pyd_types

    @staticmethod
    def match_pyarrow_pydough_types(
        field_type: pa.DataType, field_name: str
    ) -> PyDoughType:
        """
        Match the PyArrow type with its equivalent in PyDough. Raise a ValueError
        for unsupproted types in the PyDough dataframe collection.

        Unsupported datatypes:
            - Arrays/List
            - Struct
            - Dictionary
            - Binary

        Args:
            `field_type`: The PyArrow datatype for the Pandas Dataframe column

        Returns:
            Its PyDoughType equivalent
        """
        if pa.types.is_null(field_type):
            return UnknownType()

        elif pa.types.is_boolean(field_type):
            return BooleanType()

        elif (
            pa.types.is_integer(field_type)
            or pa.types.is_floating(field_type)
            or pa.types.is_decimal(field_type)
        ):
            return NumericType()

        elif pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
            return StringType()

        elif (
            pa.types.is_date(field_type)
            or pa.types.is_timestamp(field_type)
            or pa.types.is_time(field_type)
            or pa.types.is_duration(field_type)
        ):
            return DatetimeType()
        # Unsupported types
        elif pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type):
            raise ValueError(
                f"Binaries in column '{field_name}', are not supported for dataframe collections"
            )

        elif pa_types.is_list(field_type) or pa.types.is_large_list(field_type):
            raise ValueError(
                f"Arrays in column '{field_name}', are not supported for dataframe collections"
            )

        elif pa_types.is_struct(field_type):
            raise ValueError(
                f"Structs in column '{field_name}', are not supported for dataframe collections"
            )

        elif pa_types.is_dictionary(field_type):
            raise ValueError(
                f"Dictionaries in column '{field_name}', are not supported for dataframe collections"
            )

        elif pa.types.is_fixed_size_list(field_type):
            raise ValueError(
                f"Fixed-size arrays / tuples in column '{field_name}', are not supported for dataframe collections"
            )

        else:
            raise ValueError(
                f"Unknown type in column '{field_name}' for dataframe collections"
            )
