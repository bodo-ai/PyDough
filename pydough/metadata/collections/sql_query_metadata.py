"""
Definition of PyDough metadata for a collection that is defined by a SQL query.
"""

__all__ = ["SQLQueryMetadata"]

from sqlglot import parse_one

from pydough.errors import PyDoughMetadataException
from pydough.errors.error_utils import (
    HasPropertyWith,
    NoExtraKeys,
    extract_array,
    extract_string,
    is_string,
    unique_properties_predicate,
)
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.properties import (
    PropertyMetadata,
)

from .collection_metadata import CollectionMetadata


class SQLQueryMetadata(CollectionMetadata):
    """
    Concrete metadata implementation for a PyDough collection representing a
    SQL query.
    """

    # Set of names of fields that can be included in the JSON
    # object describing a simple table collection.
    allowed_fields: set[str] = CollectionMetadata.allowed_fields | {
        "query text",
        "unique properties",
    }

    def __init__(
        self,
        name: str,
        graph,
        sql_query: str,
        unique_properties: list[str | list[str]],
        description: str | None = None,
        synonyms: list[str] | None = None,
        extra_semantic_info: dict | None = None,
    ):
        super().__init__(name, graph, description, synonyms, extra_semantic_info)
        is_string.verify(sql_query, f"Property 'sql_query' of {self.error_name}")
        unique_properties_predicate.verify(
            unique_properties, f"property 'unique_properties' of {self.error_name}"
        )
        self._sql_query: str = sql_query
        self._unique_properties: list[str | list[str]] = unique_properties

    @property
    def sql_query(self) -> str:
        """
        The SQL query defining the collection.
        """
        return self._sql_query

    @property
    def unique_properties(self) -> list[str | list[str]]:
        """
        The list of all names of properties of the collection that are
        guaranteed to be unique within the collection. Entries that are a
        string represent a single column being completely unique, while entries
        that are a list of strings indicate that each combination of those
        properties is unique.
        """
        return self._unique_properties

    @staticmethod
    def create_error_name(name, graph_error_name):
        return f"SQL query collection {name!r} in {graph_error_name}"

    @property
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.sql_query)
        comp.append(self.unique_properties)
        return comp

    def verify_complete(self) -> None:
        # First do the more general checks
        super().verify_complete()

        # Extract all names properties used in the uniqueness of the table
        # collection, ensuring there are no invalid duplicates.
        malformed_unique_msg: str = f"{self.error_name} has malformed unique properties set: {self.unique_properties}"
        unique_property_combinations: set[tuple] = set()
        unique_property_names: set[str] = set()
        for unique_property in self.unique_properties:
            unique_property_set: set[str]
            if isinstance(unique_property, str):
                unique_property_set = {unique_property}
            else:
                unique_property_set = set(unique_property)
                if len(unique_property_set) < len(unique_property):
                    raise PyDoughMetadataException(malformed_unique_msg)
            unique_property_tuple: tuple = tuple(sorted(unique_property_set))
            if unique_property_tuple in unique_property_combinations:
                raise PyDoughMetadataException(malformed_unique_msg)
            unique_property_combinations.add(unique_property_tuple)
            unique_property_names.update(unique_property_set)

        # Ensure that each unique property exists as a scalar attribute of
        # the collection.
        for unique_property_name in unique_property_names:
            if unique_property_name not in self.properties:
                raise PyDoughMetadataException(
                    f"{self.error_name} does not have a property named {unique_property_name!r} to use as a unique property"
                )
            property = self.get_property(unique_property_name)
            assert isinstance(property, PropertyMetadata)
            if property.is_subcollection:
                raise PyDoughMetadataException(
                    f"{property.error_name} cannot be a unique property since it is a subcollection"
                )

    @staticmethod
    def parse_from_json(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        Parses a JSON object into the metadata for a simple table collection
        and inserts it into the graph.

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
        error_name: str = SQLQueryMetadata.create_error_name(
            collection_name, graph.error_name
        )

        # Extract the relevant properties from the JSON to build the new
        # collection, then add it to the graph.
        sql_query: str = extract_string(collection_json, "query text", error_name)
        HasPropertyWith("unique properties", unique_properties_predicate).verify(
            collection_json, error_name
        )
        unique_properties: list[str | list[str]] = collection_json["unique properties"]
        NoExtraKeys(SQLQueryMetadata.allowed_fields).verify(collection_json, error_name)
        # For the sql query, verify that it is a valid SQL statement by
        # attempting to parse it.
        try:
            parse_one(sql_query)
        except Exception as e:
            raise PyDoughMetadataException(
                f"{error_name} has an invalid SQL query: {e}"
            )
        new_collection: SQLQueryMetadata = SQLQueryMetadata(
            collection_name,
            graph,
            sql_query,
            unique_properties,
        )
        # Parse the optional common semantic properties like the description.
        new_collection.parse_optional_properties(collection_json)
        properties: list = extract_array(collection_json, "properties", error_name)
        new_collection.add_properties_from_json(properties)
        graph.add_collection(new_collection)
