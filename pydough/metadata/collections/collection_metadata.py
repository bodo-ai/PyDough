"""
Base definition of PyDough metadaata for collections.
"""

from abc import abstractmethod
from collections import defaultdict

from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    HasType,
    PyDoughMetadataException,
    is_string,
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
    - `verify_allows_property`
    - `verify_json_metadata`
    - `parse_from_json`
    """

    # Set of names of fields that can be included in the JSON
    # object describing a collection. Implementations should extend this.
    allowed_fields: set[str] = {"type", "properties"}

    def __init__(self, name: str, graph: GraphMetadata):
        from pydough.metadata.properties import (
            InheritedPropertyMetadata,
            PropertyMetadata,
        )

        is_valid_name.verify(name, "name")
        HasType(GraphMetadata).verify(graph, "graph")

        self._graph: GraphMetadata = graph
        self._name: str = name
        self._properties: dict[str, PropertyMetadata] = {}
        self._inherited_properties: dict[str, list[InheritedPropertyMetadata]] = (
            defaultdict(list)
        )
        self._definition_order: dict[str, int] = {}

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
    def inherited_properties(self):
        """
        A dictionary mapping the names of each inherited property of the
        collection (properties that are only accessible when the collection
        is accessed via a compound relationship) to the list of all inherited
        properties sharing that name.
        """
        return self._inherited_properties

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

        # Verify that no inherited properties share the same (aliased) name if
        # they come from the same source.
        for alias, inherited_properties in self.inherited_properties.items():
            sources = {p.property_inherited_from.path for p in inherited_properties}
            if len(sources) != len(inherited_properties):
                raise PyDoughMetadataException(
                    f"{self.error_name} has duplicates of inherited property {alias} that cannot be resolved"
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

    @abstractmethod
    def verify_allows_property(
        self, property: AbstractMetadata, inherited: bool
    ) -> None:
        """
        Verifies that a property is safe to add to the collection. Each
        implementation should extend this method with its own checks.

        Args:
            `property`: the metadata for a PyDough property that is being
            added to the collection.
            `inherited`: True if verifying a property being inserted as an
            inherited property, False otherwise.

        Raises:
            `PyDoughMetadataException`: if `property` is not a valid property
            to insert into the collection.
        """
        from pydough.metadata.properties import (
            InheritedPropertyMetadata,
            PropertyMetadata,
        )

        # First, make sure that the candidate property is indeed a property
        # metadata of the appropriate type.
        HasType(PropertyMetadata).verify(property, "property")
        assert isinstance(property, PropertyMetadata)
        if inherited:
            if not isinstance(property, InheritedPropertyMetadata):
                raise PyDoughMetadataException(
                    f"Expected an InheritedPropertyMetadata, received: {property.__class__.__name__}"
                )
        else:
            if isinstance(property, InheritedPropertyMetadata):
                raise PyDoughMetadataException(
                    "Cannot add an inherited property with add_property, use add_inherited_property instead."
                )

        # Verify that there is not a name conflict between an inherited
        # property and the candidate property, which would mean there is
        # an inherited property that conflicts with the name of a regular
        # property. Skip this check when inserting an inherited property
        # since collisions will occur when inheriting an inherited property.
        if not inherited and property.name in self.inherited_properties:
            raise PyDoughMetadataException(
                f"{self.inherited_properties[property.name][0].error_name} conflicts with {property.error_name}."
            )

        # Verify that there is not a name conflict between a regular property
        # and the candidate property.
        if property.name in self.properties:
            if self.properties[property.name] == property:
                raise PyDoughMetadataException(f"Already added {property.error_name}")
            if inherited:
                raise PyDoughMetadataException(
                    f"{property.error_name} conflicts with {self.properties[property.name].error_name}."
                )
            else:
                raise PyDoughMetadataException(
                    f"Duplicate property: {property.error_name} versus {self.properties[property.name].error_name}."
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
        self.verify_allows_property(property, False)
        self.properties[property.name] = property
        self.definition_order[property.name] = len(self.definition_order)

    def add_inherited_property(self, property: AbstractMetadata) -> None:
        """
        Inserts a new inherited property into the collection.

        Args:
            `property`: the metadata for a PyDough property that is being
            added to the collection as an inherited property through
            a compound relationship.

        Raises:
            `PyDoughMetadataException`: if `property` is unable to be
            inserted into the collection as an inherited property.
        """
        from pydough.metadata.properties import InheritedPropertyMetadata

        assert isinstance(property, InheritedPropertyMetadata)
        self.verify_allows_property(property, True)
        self.inherited_properties[property.name].append(property)

    def get_nouns(self) -> dict[str, list[AbstractMetadata]]:
        nouns: dict[str, list[AbstractMetadata]] = defaultdict(list)
        for property in self.properties.values():
            for noun_name, values in property.get_nouns().items():
                nouns[noun_name].extend(values)
        for property_values in self.inherited_properties.values():
            for property in property_values:
                for noun_name, values in property.get_nouns().items():
                    nouns[noun_name].extend(values)
        return nouns

    def get_property_names(self) -> list[str]:
        """
        Retrieves the names of all properties of the collection, excluding
        inherited properties.
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
            exist in the collection, or it does but as an inherited property.
        """
        if property_name not in self.properties:
            if property_name in self.inherited_properties:
                raise PyDoughMetadataException(
                    f"Cannot use get_property on inherited property {property_name!r}"
                )
            else:
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

    @staticmethod
    def verify_json_metadata(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        Generic verification that the JSON for a collection is well formed.

        Args:
            `graph`: the metadata for the graph that the collection would
            be added to.
            `collection_name`: the name of the collection that would be added
            to the graph.
            `collection_json`: the JSON object that is being verified to ensure
            it represents a valid collection.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """
        # Check that the collection name is valid string.
        is_valid_name.verify(collection_name, "collection name")

        # Check that the graph argument is indeed a graph metadata, and that the
        # name of the graph does not collide with the name of the collection.
        HasType(GraphMetadata).verify(graph, "graph")
        error_name: str = f"collection {collection_name!r} in {graph.error_name}"
        if collection_name == graph.name:
            raise PyDoughMetadataException(
                f"Cannot have collection named {collection_name!r} share the same name as the graph containing it."
            )

        # Check that the JSON data contains the required properties `type` and
        # `properties`.
        HasPropertyWith("type", is_string).verify(collection_json, error_name)
        HasPropertyWith("properties", HasType(dict)).verify(collection_json, error_name)

    @staticmethod
    def parse_from_json(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        Parses a JSON object into the metadata for a collection and inserts it
        into the graph.

        Args:
            `graph`: the metadata for the graph that the collection will be
            added to.
            `collection_name`: the name of the collection that will be added
            to the graph.
            `collection_json`: the JSON object that is being parsed to create
            the new collection.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """

        # Create the string used to identify the property in error messages.
        error_name = f"property {collection_name!r} of {graph.error_name}"

        # Verify that the JSON is well structured, in terms of generic
        # properties.
        CollectionMetadata.verify_json_metadata(graph, collection_name, collection_json)

        # Dispatch to a specific parsing procedure based on the type of
        # collection.
        property_class: type[CollectionMetadata] = (
            CollectionMetadata.get_class_for_collection_type(
                collection_json["type"], error_name
            )
        )
        property_class.parse_from_json(graph, collection_name, collection_json)
