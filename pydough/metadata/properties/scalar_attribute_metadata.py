"""
Definition of the base class for PyDough metadata for properties that
access a scalar expression of the collection.
"""

__all__ = ["ScalarAttributeMetadata"]

from abc import abstractmethod

from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import HasType
from pydough.types import PyDoughType

from .property_metadata import PropertyMetadata


class ScalarAttributeMetadata(PropertyMetadata):
    """
    Abstract base class for PyDough metadata for properties that are just
    scalars within each record of a collection, e.g. columns of tables.
    """

    def __init__(
        self, name: str, collection: CollectionMetadata, data_type: PyDoughType
    ):
        super().__init__(name, collection)
        HasType(PyDoughType).verify(data_type, "data_type")
        self._data_type: PyDoughType = data_type

    @property
    def data_type(self) -> PyDoughType:
        """
        The PyDough data type of the attribute.
        """
        return self._data_type

    @property
    @abstractmethod
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.data_type)
        return comp

    @property
    def is_plural(self) -> bool:
        return False

    @property
    def is_subcollection(self) -> bool:
        return False

    @property
    def is_reversible(self) -> bool:
        return False
