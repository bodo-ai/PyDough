"""
TODO: add file-level docstring
"""

from pydough.types import PyDoughType

from .property_metadata import PropertyMetadata


class ScalarAttributeMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str, collection, data_type: PyDoughType):
        super().__init__(name, collection)
        self.data_type = data_type

    def is_plural(self) -> bool:
        return False

    def is_subcollection(self) -> bool:
        return False

    def is_reversible(self) -> bool:
        return False
