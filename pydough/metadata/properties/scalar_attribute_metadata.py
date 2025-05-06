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
        self,
        name: str,
        collection: CollectionMetadata,
        data_type: PyDoughType,
        sample_values: list | None,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ):
        super().__init__(name, collection, description, synonyms, extra_semantic_info)
        HasType(PyDoughType).verify(data_type, "data_type")
        self._sample_values: list | None = sample_values
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

    @property
    def sample_values(self) -> list | None:
        """
        A list of sample values for the attribute, if it exists.
        """
        return self._sample_values
