"""
Base definition of PyDough metadaata for collections.
"""

from abc import abstractmethod

from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.errors import (
    HasType,
    PyDoughMetadataException,
    extract_string,
    is_valid_name,
)
from pydough.metadata.graphs import GraphMetadata


class CollectionMetadata(AbstractMetadata):
    """
    Abstract base class for PyDough metadata for collections.

    Each implementation must include the following APIs:
    - `create_error_name`
    - `components`
    - `verify_complete`
    - `parse_from_json`
    """

    # Set of names of fields that can be included in the JSON
    # object describing a collection. Implementations should extend this.
    allowed_fields: set[str] = {
        "name",
        "type",
        "properties",
        "description",
        "synonyms",
        "extra semantic info",
    }

    def __init__(
        self,
        name: str,
        graph: GraphMetadata,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ):
        from pydough.metadata.properties import (
            PropertyMetadata,
        )

        is_valid_name.verify(name, "name")
        HasType(GraphMetadata).verify(graph, "graph")

        self._graph: GraphMetadata = graph
        self._name: str = name
        self._properties: dict[str, PropertyMetadata] = {}
        self._definition_order: dict[str, int] = {}
        super().__init__(description, synonyms, extra_semantic_info)

    @property
    def graph(self) -> GraphMetadata:
        """
        The graph that the collection belongs to.
        """
        return self._graph

    @property
    def name(self) -> str:
        """
        The name of the collection.
        """
        return self._name

    @property
    def properties(self):
        """
        A dictionary mapping the names of each property of the collection to
        the property metadata.
        """
        return self._properties

    @property
    def definition_order(self):
        """
        A dictionary mapping each property added to the collection to the time
        when it was added as an index, so the properties can be re-accessed
        later in the same order.
        """
        return self._definition_order

    @property
    def path(self) -> str:
        return f"{self.graph.path}.{self.name}"

    @property
    def error_name(self):
        return self.create_error_name(self.name, self.graph.error_name)

    @staticmethod
    @abstractmethod
    def create_error_name(name: str, graph_error_name: str):
        """
        Creates a string used for the purposes of the `error_name` property.

        Args:
            `name`: the name of the collection.
            `graph_error_name`: the error_name property of the graph containing
            the collection.

        Returns:
            The string to use to identify the collection in exception messages.
        """

    @property
    @abstractmethod
    def components(self) -> list:
        comp: list = self.graph.components
        comp.append(self.name)
        return comp

    @abstractmethod
    def verify_complete(self) -> None:
        """
        Verifies that a collection is well-formed after the parsing of all of
        its properties is complete. Subclasses should extend the checks done
        in the default implementation.

        Raises:
            `PyDoughMetadataException`: if the collection is malformed in any
            way after parsing is done.
        """
        from pydough.metadata.properties.subcollection_relationship_metadata import (
            SubcollectionRelationshipMetadata,
        )

        # Verify that the name relationships are well formed.
        if self.graph.get_collection(self.name) is not self:
            raise PyDoughMetadataException(
                f"{self.error_name} does not match correctly with the collection names in {self.graph.error_name}"
            )
        for property_name, property in self.properties.items():
            if property.name != property_name:
                raise PyDoughMetadataException(
                    f"{property.error_name} does not match correctly with the property names in {self.error_name}"
                )

        # Verify that all properties are well formed with regards to their
        # cardinality relationships
        for property in self.properties.values():
            if isinstance(property, SubcollectionRelationshipMetadata):
                if not property.is_subcollection:
                    raise PyDoughMetadataException(
                        f"{property.error_name} should be a subcollection but is not"
                    )
            else:
                if property.is_subcollection:
                    raise PyDoughMetadataException(
                        f"{property.error_name} should not be a subcollection but is"
                    )
                if property.is_plural:
                    raise PyDoughMetadataException(
                        f"{property.error_name} should not be plural but is"
                    )

    def add_property(self, property: AbstractMetadata) -> None:
        """
        Inserts a new property into the collection.

        Args:
            `property`: the metadata for a PyDough property that is being
            added to the collection.

        Raises:
            `PyDoughMetadataException`: if `property` is unable to be
            inserted into the collection.
        """
        from pydough.metadata.properties import PropertyMetadata

        assert isinstance(property, PropertyMetadata)
        if property.name in self.properties:
            raise PyDoughMetadataException(
                f"Duplicate property: {property.error_name} versus {self.properties[property.name].error_name}."
            )
        self.properties[property.name] = property
        self.definition_order[property.name] = len(self.definition_order)

    def get_property_names(self) -> list[str]:
        """
        Retrieves the names of all properties of the collection.
        """
        return list(self.properties)

    def get_property(self, property_name: str) -> AbstractMetadata:
        """
        Fetches a property from the collection by name.

        Args:
            `property_name`: the name of the property being requested.

        Returns:
            The metadata for the requested property.

        Raises:
            `PyDoughMetadataException`: if a property with name `name` does not
            exist in the collection.
        """
        if property_name not in self.properties:
            raise PyDoughMetadataException(
                f"{self.error_name} does not have a property {property_name!r}"
            )
        return self.properties[property_name]

    def __getitem__(self, key: str):
        return self.get_property(key)

    @staticmethod
    def get_class_for_collection_type(
        name: str, error_name: str
    ) -> type["CollectionMetadata"]:
        """
        Fetches the PropertyType implementation class for a string
        representation of the collection type.

        Args:
            `name`: the string representation of a collection type.
            `error_name`: the string used in error messages to describe
            the object that `name` came from.

        Returns:
            The class of the property type corresponding to `name`.

        Raises:
            `PyDoughMetadataException` if the string does not correspond
            to a known class type.
        """
        from .simple_table_metadata import SimpleTableMetadata

        match name:
            case "simple_table":
                return SimpleTableMetadata
            case property_type:
                raise PyDoughMetadataException(
                    f"Unrecognized collection type for {error_name}: {repr(property_type)}"
                )

    def add_properties_from_json(self, properties_json: list) -> None:
        """
        Insert the scalar properties from the JSON for collection into the
        metadata object for the collection.

        Args:
            `properties_json`: the list of JSON objects, each representing a
            scalar property that should be parsed and inserted into the
            collection.
        """
        from pydough.metadata.properties import TableColumnMetadata

        for property_json in properties_json:
            # Extract the name/type, and create the string used to identify
            # the property in error messages.
            property_name: str = extract_string(
                property_json, "name", f"property of {self.error_name}"
            )
            error_name = f"property {property_json['name']!r} of {self.error_name}"
            property_type: str = extract_string(property_json, "type", error_name)
            # Dispatch to the correct implementation based on the type.
            match property_type:
                case "table column":
                    TableColumnMetadata.parse_from_json(
                        self, property_name, property_json
                    )
                case _:
                    raise PyDoughMetadataException(
                        f"Unrecognized property type {property_type!r} for {error_name}"
                    )
