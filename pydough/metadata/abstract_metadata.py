"""
The definition of the base class for all PyDough metadata.
"""

__all__ = ["AbstractMetadata"]

from abc import ABC, abstractmethod


class AbstractMetadata(ABC):
    """
    The abstract base class used to define all PyDough metadata classes for
    graphs, collections, and properties. Each class must include the following
    APIs:

    - `error_name`
    - `components`
    - `get_nouns`
    """

    @property
    @abstractmethod
    def error_name(self) -> str:
        """
        A string used to identify the metadata object when displayed in an
        error message.
        """

    @property
    @abstractmethod
    def components(self) -> list:
        """
        A list of objects used to uniquely identify a metadata object
        by equality.
        """

    @abstractmethod
    def get_nouns(self) -> dict[str, list["AbstractMetadata"]]:
        """
        Fetches all of the names of nouns accessible from the metadata for
        a PyDough graph, collection, or property.

        Returns:
            A dictionary mapping the name of each noun to a list of all
            metadata objects with that name.
        """

    @property
    @abstractmethod
    def path(self) -> str:
        """
        A string used as a shorthand to identify the metadata object and its
        ancestry.
        """

    def __eq__(self, other):
        return type(self) is type(other) and self.components == other.components

    def __repr__(self):
        return f"PyDoughMetadata[{self.error_name}]"
