"""
TODO: add file-level docstring
"""

from pydough.types import PyDoughType
from pydough.metadata.errors import verify_has_type

from . import PropertyMetadata
from pydough.metadata.collections import CollectionMetadata
from abc import abstractmethod


class ScalarAttributeMetadata(PropertyMetadata):
    """
    Abstract base class for PyDough metadata for properties that are just
    scalars within each record of a collection, e.g. columns of tables.
    """

    def __init__(
        self, name: str, collection: CollectionMetadata, data_type: PyDoughType
    ):
        super().__init__(name, collection)
        verify_has_type(data_type, PyDoughType, "data_type")
        self._data_type: PyDoughType = data_type

    @property
    def data_type(self) -> PyDoughType:
        """
        The PyDough data type of the attribute.
        """
        return self._data_type

    @property
    @abstractmethod
    def components(self) -> tuple:
        return super().components + (self.data_type,)

    @property
    def is_plural(self) -> bool:
        return False

    @property
    def is_subcollection(self) -> bool:
        return False

    @property
    def is_reversible(self) -> bool:
        return False
