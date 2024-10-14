from typing import Dict, Tuple
from .property_metadata import PropertyMetadata
from pydough.types import PyDoughType


class TableColumnMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        graph_name: str,
        collection_name: str,
        name: str,
        column_name: str,
        data_type: PyDoughType,
    ):
        super().__init__(graph_name, collection_name, name)
        self.column_name = column_name
        self.data_type = data_type

    def components(self) -> Tuple:
        """
        TODO: add function doscstring.
        """
        return super().components() + (self.column_name, self.data_type)

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, properties_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        raise NotImplementedError
