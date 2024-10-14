"""
TODO: add file-level docstring
"""

from typing import Tuple
from .property_metadata import PropertyMetadata


class InheritedPropertyMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self, name: str, parent_collection_name: str, parent_property_name: str
    ):
        super().__init__(name)
        self.parent_collection_name = parent_collection_name
        self.parent_property_name = parent_property_name

    def components(self) -> Tuple:
        """
        TODO: add function doscstring.
        """
        return super().components() + (
            self.parent_collection_name,
            self.parent_property_name,
        )
