"""
TODO: add file-level docstring
"""

from abc import ABC, abstractmethod

from typing import Dict, List


class AbstractMetadata(ABC):
    """
    TODO: add class docstring
    """

    @property
    @abstractmethod
    def error_name(self) -> str:
        """
        Returns a string used to identify a metadata object when displayed
        in an error message.
        """

    @property
    @abstractmethod
    def components(self) -> tuple:
        """
        Returns a tuple of objects used to uniquely identify a metadata object
        by equality.
        """

    @abstractmethod
    def get_nouns(self) -> Dict[str, List["AbstractMetadata"]]:
        """
        Fetches all of the names of nouns accessible from the metadata for
        a PyDough graph, collection, or property.
        """

    def __eq__(self, other):
        return type(self) is type(other) and self.components == other.components

    def __repr__(self, other):
        return f"PyDoughMetadata[{self.error_name}]"
