"""
Error-handling unit tests for the PyDough metadata module.
"""

import ast
import re

import pytest

from pydough import parse_json_metadata_from_file
from pydough.configs import PyDoughConfigs
from pydough.errors import PyDoughMetadataException, PyDoughTypeException
from pydough.metadata import CollectionMetadata, GraphMetadata
from pydough.unqualified import UnqualifiedNode, qualify_node, transform_code
from tests.testing_utilities import graph_fetcher


def test_missing_collection(get_sample_graph: graph_fetcher) -> None:
    """
    Testing the error handling for trying to fetch a collection that does not
    exist.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    with pytest.raises(
        PyDoughMetadataException,
        match="graph 'TPCH' does not have a collection named 'Inventory'",
    ):
        graph.get_collection("Inventory")


def test_missing_property(get_sample_graph: graph_fetcher) -> None:
    """
    Testing the error handling for trying to fetch a collection that does not
    exist.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    with pytest.raises(
        PyDoughMetadataException,
        match="simple table collection 'parts' in graph 'TPCH' does not have a property 'color'",
    ):
        collection = graph.get_collection("parts")
        assert isinstance(collection, CollectionMetadata)
        collection.get_property("color")


@pytest.mark.parametrize(
    "graph_name, error_message",
    [
        pytest.param(
            "NO_EXIST",
            "PyDough metadata file located at '(.+)' does not contain a graph named 'NO_EXIST'",
            id="missing_graph",
        ),
        pytest.param(
            "#BadGraphName",
            "graph name must be a string that is a Python identifier",
            id="#BadGraphName",
        ),
        pytest.param(
            "MISSING_VERSION",
            "metadata for PyDough graph must be a JSON object containing a field 'version' and field 'version' must be a string",
            id="MISSING_VERSION",
        ),
        pytest.param(
            "BAD_VERSION",
            "Unrecognized PyDough metadata version: 'HelloWorld'",
            id="BAD_VERSION",
        ),
        pytest.param(
            "MISSING_COLLECTIONS",
            "graph 'MISSING_COLLECTIONS' must be a JSON object containing a field 'collections' and field 'collections' must be a JSON array",
            id="MISSING_COLLECTIONS",
        ),
        pytest.param(
            "MISSING_RELATIONSHIPS",
            "graph 'MISSING_RELATIONSHIPS' must be a JSON object containing a field 'relationships' and field 'relationships' must be a JSON array",
            id="MISSING_RELATIONSHIPS",
        ),
        pytest.param(
            "EXTRA_GRAPH_FIELDS",
            re.escape(
                "graph 'EXTRA_GRAPH_FIELDS' must be a JSON object containing no fields except for ['additional definitions', 'collections', 'extra semantic info', 'functions', 'name', 'relationships', 'verified pydough analysis', 'version']"
            ),
            id="EXTRA_GRAPH_FIELDS",
        ),
        pytest.param(
            "BAD_COLLECTION_NAME_1",
            "name must be a string that is a Python identifier",
            id="BAD_COLLECTION_NAME_1",
        ),
        pytest.param(
            "BAD_COLLECTION_NAME_2",
            "name must be a string that is a Python identifier",
            id="BAD_COLLECTION_NAME_2",
        ),
        pytest.param(
            "BAD_PROPERTY_NAME_1",
            "name must be a string that is a Python identifier",
            id="BAD_PROPERTY_NAME_1",
        ),
        pytest.param(
            "BAD_PROPERTY_NAME_2",
            "name must be a string that is a Python identifier",
            id="BAD_PROPERTY_NAME_2",
        ),
        pytest.param(
            "BAD_RELATIONSHIP_NAME",
            "metadata for relationships within graph 'BAD_RELATIONSHIP_NAME' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_RELATIONSHIP_NAME",
        ),
        pytest.param(
            "MISSING_COLLECTION_NAME",
            "metadata for collections within graph 'MISSING_COLLECTION_NAME' must be a JSON object containing a field 'name' and field 'name' must be a string",
            id="MISSING_COLLECTION_NAME",
        ),
        pytest.param(
            "MISSING_COLLECTION_TYPE",
            "metadata for collections within graph 'MISSING_COLLECTION_TYPE' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="MISSING_COLLECTION_TYPE",
        ),
        pytest.param(
            "BAD_COLLECTION_TYPE_1",
            "metadata for collections within graph 'BAD_COLLECTION_TYPE_1' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_COLLECTION_TYPE_1",
        ),
        pytest.param(
            "BAD_COLLECTION_TYPE_2",
            "Unrecognized PyDough collection type for collection 'collection': 'no collection type'",
            id="BAD_COLLECTION_TYPE_2",
        ),
        pytest.param(
            "MISSING_COLLECTION_TABLE_PATH",
            "simple table collection 'collection' in graph 'MISSING_COLLECTION_TABLE_PATH' must be a JSON object containing a field 'table path' and field 'table path' must be a string",
            id="MISSING_COLLECTION_TABLE_PATH",
        ),
        pytest.param(
            "BAD_COLLECTION_TABLE_PATH",
            "simple table collection 'collection' in graph 'BAD_COLLECTION_TABLE_PATH' must be a JSON object containing a field 'table path' and field 'table path' must be a string",
            id="BAD_COLLECTION_TABLE_PATH",
        ),
        pytest.param(
            "MISSING_COLLECTION_UNIQUE_PROPERTIES",
            "simple table collection 'collection' in graph 'MISSING_COLLECTION_UNIQUE_PROPERTIES' must be a JSON object containing a field 'unique properties' and field 'unique properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="MISSING_COLLECTION_UNIQUE_PROPERTIES",
        ),
        pytest.param(
            "BAD_COLLECTION_UNIQUE_PROPERTIES_1",
            "simple table collection 'collection' in graph 'BAD_COLLECTION_UNIQUE_PROPERTIES_1' must be a JSON object containing a field 'unique properties' and field 'unique properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="BAD_COLLECTION_UNIQUE_PROPERTIES_1",
        ),
        pytest.param(
            "BAD_COLLECTION_UNIQUE_PROPERTIES_2",
            "simple table collection 'collection' in graph 'BAD_COLLECTION_UNIQUE_PROPERTIES_2' must be a JSON object containing a field 'unique properties' and field 'unique properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="BAD_COLLECTION_UNIQUE_PROPERTIES_2",
        ),
        pytest.param(
            "BAD_COLLECTION_UNIQUE_PROPERTIES_3",
            "simple table collection 'collection' in graph 'BAD_COLLECTION_UNIQUE_PROPERTIES_3' must be a JSON object containing a field 'unique properties' and field 'unique properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="BAD_COLLECTION_UNIQUE_PROPERTIES_3",
        ),
        pytest.param(
            "BAD_COLLECTION_UNIQUE_PROPERTIES_4",
            "simple table collection 'collection' in graph 'BAD_COLLECTION_UNIQUE_PROPERTIES_4' must be a JSON object containing a field 'unique properties' and field 'unique properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="BAD_COLLECTION_UNIQUE_PROPERTIES_4",
        ),
        pytest.param(
            "BAD_COLLECTION_UNIQUE_PROPERTIES_5",
            "simple table collection 'collection' in graph 'BAD_COLLECTION_UNIQUE_PROPERTIES_5' must be a JSON object containing a field 'unique properties' and field 'unique properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="BAD_COLLECTION_UNIQUE_PROPERTIES_5",
        ),
        pytest.param(
            "BAD_COLLECTION_UNIQUE_PROPERTIES_6",
            "simple table collection 'collection' in graph 'BAD_COLLECTION_UNIQUE_PROPERTIES_6' does not have a property named 'Foo' to use as a unique property",
            id="BAD_COLLECTION_UNIQUE_PROPERTIES_6",
        ),
        pytest.param(
            "MISSING_COLLECTION_PROPERTIES",
            "simple table collection 'collection' in graph 'MISSING_COLLECTION_PROPERTIES' must be a JSON object containing a field 'properties' and field 'properties' must be a JSON array",
            id="MISSING_COLLECTION_PROPERTIES",
        ),
        pytest.param(
            "BAD_COLLECTION_PROPERTIES",
            "simple table collection 'collection' in graph 'BAD_COLLECTION_PROPERTIES' must be a JSON object containing a field 'properties' and field 'properties' must be a JSON array",
            id="BAD_COLLECTION_PROPERTIES",
        ),
        pytest.param(
            "MISSING_PROPERTIES_NAME",
            "property of simple table collection 'collection' in graph 'MISSING_PROPERTIES_NAME' must be a JSON object containing a field 'name' and field 'name' must be a string",
            id="MISSING_PROPERTIES_NAME",
        ),
        pytest.param(
            "MISSING_PROPERTIES_TYPE",
            "property 'key' of simple table collection 'collection' in graph 'MISSING_PROPERTIES_TYPE' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="MISSING_PROPERTIES_TYPE",
        ),
        pytest.param(
            "BAD_PROPERTIES_TYPE_1",
            "property 'key' of simple table collection 'collection' in graph 'BAD_PROPERTIES_TYPE_1' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_PROPERTIES_TYPE_1",
        ),
        pytest.param(
            "BAD_PROPERTIES_TYPE_2",
            "Unrecognized property type 'unknown property type' for property 'key' of simple table collection 'collection' in graph 'BAD_PROPERTIES_TYPE_2'",
            id="BAD_PROPERTIES_TYPE_2",
        ),
        pytest.param(
            "MISSING_TABLE_COLUMN_DATA_TYPE",
            "table column property 'key' of simple table collection 'collection' in graph 'MISSING_TABLE_COLUMN_DATA_TYPE' must be a JSON object containing a field 'data type' and field 'data type' must be a string",
            id="MISSING_TABLE_COLUMN_DATA_TYPE",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_DATA_TYPE_1",
            "table column property 'key' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_DATA_TYPE_1' must be a JSON object containing a field 'data type' and field 'data type' must be a string",
            id="BAD_TABLE_COLUMN_DATA_TYPE_1",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_DATA_TYPE_2",
            "Unrecognized type string 'foobar'",
            id="BAD_TABLE_COLUMN_DATA_TYPE_2",
        ),
        pytest.param(
            "MISSING_TABLE_COLUMN_COLUMN_NAME",
            "table column property 'key' of simple table collection 'collection' in graph 'MISSING_TABLE_COLUMN_COLUMN_NAME' must be a JSON object containing a field 'column name' and field 'column name' must be a string",
            id="MISSING_TABLE_COLUMN_COLUMN_NAME",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_COLUMN_NAME",
            "table column property 'key' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_COLUMN_NAME' must be a JSON object containing a field 'column name' and field 'column name' must be a string",
            id="BAD_TABLE_COLUMN_COLUMN_NAME",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_DESCRIPTION",
            "table column property 'key' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_DESCRIPTION' must be a JSON object containing a field 'description' and field 'description' must be a string",
            id="BAD_TABLE_COLUMN_DESCRIPTION",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_SYNONYMS",
            "table column property 'key' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_SYNONYMS' must be a JSON object containing a field 'synonyms' and field 'synonyms' must be a JSON array",
            id="BAD_TABLE_COLUMN_SYNONYMS",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_SAMPLE_VALUES",
            "table column property 'key' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_SAMPLE_VALUES' must be a JSON object containing a field 'sample values' and field 'sample values' must be a JSON array",
            id="BAD_TABLE_COLUMN_SAMPLE_VALUES",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_EXTRA_SEMANTIC_INFO",
            "table column property 'key' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_EXTRA_SEMANTIC_INFO' must be a JSON object containing a field 'extra semantic info' and field 'extra semantic info' must be a JSON object",
            id="BAD_TABLE_COLUMN_EXTRA_SEMANTIC_INFO",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_EXTRA_FIELDS",
            re.escape(
                "table column property 'key' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_EXTRA_FIELDS' must be a JSON object containing no fields except for ['column name', 'data type', 'description', 'extra semantic info', 'name', 'sample values', 'synonyms', 'type']"
            ),
            id="BAD_TABLE_COLUMN_EXTRA_FIELDS",
        ),
        pytest.param(
            "BAD_SIMPLE_TABLE_DESCRIPTION",
            "simple table collection 'collection' in graph 'BAD_SIMPLE_TABLE_DESCRIPTION' must be a JSON object containing a field 'description' and field 'description' must be a string",
            id="BAD_SIMPLE_TABLE_DESCRIPTION",
        ),
        pytest.param(
            "BAD_SIMPLE_TABLE_SYNONYMS",
            "simple table collection 'collection' in graph 'BAD_SIMPLE_TABLE_SYNONYMS' must be a JSON object containing a field 'synonyms' and field 'synonyms' must be a JSON array",
            id="BAD_SIMPLE_TABLE_SYNONYMS",
        ),
        pytest.param(
            "BAD_SIMPLE_TABLE_EXTRA_SEMANTIC_INFO",
            "simple table collection 'collection' in graph 'BAD_SIMPLE_TABLE_EXTRA_SEMANTIC_INFO' must be a JSON object containing a field 'extra semantic info' and field 'extra semantic info' must be a JSON object",
            id="BAD_SIMPLE_TABLE_EXTRA_SEMANTIC_INFO",
        ),
        pytest.param(
            "BAD_SIMPLE_TABLE_EXTRA_FIELDS",
            re.escape(
                "simple table collection 'collection' in graph 'BAD_SIMPLE_TABLE_EXTRA_FIELDS' must be a JSON object containing no fields except for ['description', 'extra semantic info', 'name', 'properties', 'synonyms', 'table path', 'type', 'unique properties']"
            ),
            id="BAD_SIMPLE_TABLE_EXTRA_FIELDS",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_MISSING_PARENT",
            "metadata for property 'cross' within graph 'BAD_CARTESIAN_PRODUCT_MISSING_PARENT' must be a JSON object containing a field 'parent collection' and field 'parent collection' must be a string",
            id="BAD_CARTESIAN_PRODUCT_MISSING_PARENT",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_MISSING_CHILD",
            "metadata for property 'cross' within graph 'BAD_CARTESIAN_PRODUCT_MISSING_CHILD' must be a JSON object containing a field 'child collection' and field 'child collection' must be a string",
            id="BAD_CARTESIAN_PRODUCT_MISSING_CHILD",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_INVALID_PARENT_1",
            "metadata for property 'cross' within graph 'BAD_CARTESIAN_PRODUCT_INVALID_PARENT_1' must be a JSON object containing a field 'parent collection' and field 'parent collection' must be a string",
            id="BAD_CARTESIAN_PRODUCT_INVALID_PARENT_1",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_INVALID_PARENT_2",
            "graph 'BAD_CARTESIAN_PRODUCT_INVALID_PARENT_2' does not have a collection named 'fake_collection_name'",
            id="BAD_CARTESIAN_PRODUCT_INVALID_PARENT_2",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_INVALID_CHILD_1",
            "metadata for property 'cross' within graph 'BAD_CARTESIAN_PRODUCT_INVALID_CHILD_1' must be a JSON object containing a field 'child collection' and field 'child collection' must be a string",
            id="BAD_CARTESIAN_PRODUCT_INVALID_CHILD_1",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_INVALID_CHILD_2",
            "graph 'BAD_CARTESIAN_PRODUCT_INVALID_CHILD_2' does not have a collection named 'fake_collection_name'",
            id="BAD_CARTESIAN_PRODUCT_INVALID_CHILD_2",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_DESCRIPTION",
            "cartesian property 'cross' of simple table collection 'parent' in graph 'BAD_CARTESIAN_PRODUCT_DESCRIPTION' must be a JSON object containing a field 'description' and field 'description' must be a string",
            id="BAD_CARTESIAN_PRODUCT_DESCRIPTION",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_ALWAYS_MATCHES",
            "cartesian property 'cross' of simple table collection 'parent' in graph 'BAD_CARTESIAN_PRODUCT_ALWAYS_MATCHES' must be a JSON object containing a field 'always matches' and field 'always matches' must be a boolean",
            id="BAD_CARTESIAN_PRODUCT_ALWAYS_MATCHES",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_SYNONYMS",
            "cartesian property 'cross' of simple table collection 'parent' in graph 'BAD_CARTESIAN_PRODUCT_SYNONYMS' must be a JSON object containing a field 'synonyms' and field 'synonyms' must be a JSON array",
            id="BAD_CARTESIAN_PRODUCT_SYNONYMS",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_EXTRA_SEMANTIC_INFO",
            "cartesian property 'cross' of simple table collection 'parent' in graph 'BAD_CARTESIAN_PRODUCT_EXTRA_SEMANTIC_INFO' must be a JSON object containing a field 'extra semantic info' and field 'extra semantic info' must be a JSON object",
            id="BAD_CARTESIAN_PRODUCT_EXTRA_SEMANTIC_INFO",
        ),
        pytest.param(
            "BAD_CARTESIAN_PRODUCT_EXTRA_FIELDS",
            re.escape(
                "cartesian property 'cross' of simple table collection 'parent' in graph 'BAD_CARTESIAN_PRODUCT_EXTRA_FIELDS' must be a JSON object containing no fields except for ['always matches', 'child collection', 'description', 'extra semantic info', 'name', 'parent collection', 'synonyms', 'type']"
            ),
            id="BAD_CARTESIAN_PRODUCT_EXTRA_FIELDS",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_MISSING_PARENT",
            "metadata for property 'children' within graph 'BAD_SIMPLE_JOIN_MISSING_PARENT' must be a JSON object containing a field 'parent collection' and field 'parent collection' must be a string",
            id="BAD_SIMPLE_JOIN_MISSING_PARENT",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_PARENT_1",
            "metadata for property 'children' within graph 'BAD_SIMPLE_JOIN_INVALID_PARENT_1' must be a JSON object containing a field 'parent collection' and field 'parent collection' must be a string",
            id="BAD_SIMPLE_JOIN_INVALID_PARENT_1",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_PARENT_2",
            "graph 'BAD_SIMPLE_JOIN_INVALID_PARENT_2' does not have a collection named 'bad_collection_name'",
            id="BAD_SIMPLE_JOIN_INVALID_PARENT_2",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_MISSING_CHILD",
            "metadata for property 'children' within graph 'BAD_SIMPLE_JOIN_MISSING_CHILD' must be a JSON object containing a field 'child collection' and field 'child collection' must be a string",
            id="BAD_SIMPLE_JOIN_MISSING_CHILD",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_CHILD_1",
            "metadata for property 'children' within graph 'BAD_SIMPLE_JOIN_INVALID_CHILD_1' must be a JSON object containing a field 'child collection' and field 'child collection' must be a string",
            id="BAD_SIMPLE_JOIN_INVALID_CHILD_1",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_CHILD_2",
            "graph 'BAD_SIMPLE_JOIN_INVALID_CHILD_2' does not have a collection named 'bad_collection_name'",
            id="BAD_SIMPLE_JOIN_INVALID_CHILD_2",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_MISSING_KEYS",
            "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_MISSING_KEYS' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="BAD_SIMPLE_JOIN_MISSING_KEYS",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_KEYS_1",
            "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_INVALID_KEYS_1' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="BAD_SIMPLE_JOIN_INVALID_KEYS_1",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_KEYS_2",
            "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_INVALID_KEYS_2' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="BAD_SIMPLE_JOIN_INVALID_KEYS_2",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_KEYS_3",
            "simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_INVALID_KEYS_3' does not have a property 'foo'",
            id="BAD_SIMPLE_JOIN_INVALID_KEYS_3",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_KEYS_4",
            "simple table collection 'child' in graph 'BAD_SIMPLE_JOIN_INVALID_KEYS_4' does not have a property 'bar'",
            id="BAD_SIMPLE_JOIN_INVALID_KEYS_4",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_KEYS_5",
            "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_INVALID_KEYS_5' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="BAD_SIMPLE_JOIN_INVALID_KEYS_5",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_KEYS_6",
            "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_INVALID_KEYS_6' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="BAD_SIMPLE_JOIN_INVALID_KEYS_6",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_MISSING_SINGULAR",
            "metadata for property children within graph 'BAD_SIMPLE_JOIN_MISSING_SINGULAR' must be a JSON object containing a field 'singular' and field 'singular' must be a boolean",
            id="BAD_SIMPLE_JOIN_MISSING_SINGULAR",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_INVALID_SINGULAR",
            "metadata for property children within graph 'BAD_SIMPLE_JOIN_INVALID_SINGULAR' must be a JSON object containing a field 'singular' and field 'singular' must be a boolean",
            id="BAD_SIMPLE_JOIN_INVALID_SINGULAR",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_DESCRIPTION",
            "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_DESCRIPTION' must be a JSON object containing a field 'description' and field 'description' must be a string",
            id="BAD_SIMPLE_JOIN_DESCRIPTION",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_SYNONYMS",
            "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_SYNONYMS' must be a JSON object containing a field 'synonyms' and field 'synonyms' must be a JSON array",
            id="BAD_SIMPLE_JOIN_SYNONYMS",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_EXTRA_SEMANTIC_INFO",
            "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_EXTRA_SEMANTIC_INFO' must be a JSON object containing a field 'extra semantic info' and field 'extra semantic info' must be a JSON object",
            id="BAD_SIMPLE_JOIN_EXTRA_SEMANTIC_INFO",
        ),
        pytest.param(
            "BAD_SIMPLE_JOIN_EXTRA_FIELDS",
            re.escape(
                "simple join property 'children' of simple table collection 'parent' in graph 'BAD_SIMPLE_JOIN_EXTRA_FIELDS' must be a JSON object containing no fields except for ['always matches', 'child collection', 'description', 'extra semantic info', 'keys', 'name', 'parent collection', 'singular', 'synonyms', 'type']"
            ),
            id="BAD_SIMPLE_JOIN_EXTRA_FIELDS",
        ),
        pytest.param(
            "DUPLICATE_RELATIONSHIP_NAMES",
            "Duplicate property: cartesian property 'children' of simple table collection 'parent' in graph 'DUPLICATE_RELATIONSHIP_NAMES' versus simple join property 'children' of simple table collection 'parent' in graph 'DUPLICATE_RELATIONSHIP_NAMES'.",
            id="DUPLICATE_RELATIONSHIP_NAMES",
        ),
        pytest.param(
            "DUPLICATE_PROPERTY_NAMES",
            "Duplicate property: table column property 'key' of simple table collection 'parent' in graph 'DUPLICATE_PROPERTY_NAMES' versus table column property 'key' of simple table collection 'parent' in graph 'DUPLICATE_PROPERTY_NAMES'.",
            id="DUPLICATE_PROPERTY_NAMES",
        ),
        pytest.param(
            "DUPLICATE_COLLECTION_NAMES",
            "Duplicate collections: simple table collection 'parent' in graph 'DUPLICATE_COLLECTION_NAMES' versus simple table collection 'parent' in graph 'DUPLICATE_COLLECTION_NAMES'",
            id="DUPLICATE_COLLECTION_NAMES",
        ),
        pytest.param(
            "BAD_FUNCTIONS_LIST",
            "graph 'BAD_FUNCTIONS_LIST' must be a JSON object containing a field 'functions' and field 'functions' must be a JSON array",
            id="BAD_FUNCTIONS_LIST",
        ),
        pytest.param(
            "BAD_FUNCTION_MISSING_NAME",
            "metadata for UDF definition within graph 'BAD_FUNCTION_MISSING_NAME' must be a JSON object containing a field 'name' and field 'name' must be a string",
            id="BAD_FUNCTION_MISSING_NAME",
        ),
        pytest.param(
            "BAD_FUNCTION_BAD_NAME_1",
            "metadata for UDF definition within graph 'BAD_FUNCTION_BAD_NAME_1' must be a JSON object containing a field 'name' and field 'name' must be a string",
            id="BAD_FUNCTION_BAD_NAME_1",
        ),
        pytest.param(
            "BAD_FUNCTION_BAD_NAME_2",
            "function name must be a string that is a Python identifier",
            id="BAD_FUNCTION_BAD_NAME_2",
        ),
        pytest.param(
            "BAD_FUNCTION_BAD_NAME_3",
            "Function name 'BAD_FUNCTION_BAD_NAME_3' cannot be the same as the graph name 'BAD_FUNCTION_BAD_NAME_3'",
            id="BAD_FUNCTION_BAD_NAME_3",
        ),
        pytest.param(
            "BAD_FUNCTION_BAD_NAME_4",
            "Function name 'regions' cannot be the same as a collection name in graph 'BAD_FUNCTION_BAD_NAME_4'",
            id="BAD_FUNCTION_BAD_NAME_4",
        ),
        pytest.param(
            "BAD_FUNCTION_DUPLICATE_NAME",
            "Function 'FOO' already exists in graph 'BAD_FUNCTION_DUPLICATE_NAME'",
            id="BAD_FUNCTION_DUPLICATE_NAME",
        ),
        pytest.param(
            "BAD_FUNCTION_MISSING_TYPE",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_MISSING_TYPE' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_FUNCTION_MISSING_TYPE",
        ),
        pytest.param(
            "BAD_FUNCTION_BAD_TYPE_1",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_BAD_TYPE_1' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_FUNCTION_BAD_TYPE_1",
        ),
        pytest.param(
            "BAD_FUNCTION_BAD_TYPE_2",
            "Unrecognized PyDough function type for metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_BAD_TYPE_2': 'foobar'",
            id="BAD_FUNCTION_BAD_TYPE_2",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_ALIAS_MISSING_SQL_FUNCTION",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_ALIAS_MISSING_SQL_FUNCTION' must be a JSON object containing a field 'sql function' and field 'sql function' must be a string",
            id="BAD_FUNCTION_SQL_ALIAS_MISSING_SQL_FUNCTION",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_ALIAS_BAD_SQL_FUNCTION",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_ALIAS_BAD_SQL_FUNCTION' must be a JSON object containing a field 'sql function' and field 'sql function' must be a string",
            id="BAD_FUNCTION_SQL_ALIAS_BAD_SQL_FUNCTION",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_ALIAS_BAD_AGGREGATION",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_ALIAS_BAD_AGGREGATION' must be a JSON object containing a field 'aggregation' and field 'aggregation' must be a boolean",
            id="BAD_FUNCTION_SQL_ALIAS_BAD_AGGREGATION",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_ALIAS_BAD_DESCRIPTION",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_ALIAS_BAD_DESCRIPTION' must be a JSON object containing a field 'description' and field 'description' must be a string",
            id="BAD_FUNCTION_SQL_ALIAS_BAD_DESCRIPTION",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_ALIAS_EXTRA_FIELD",
            re.escape(
                "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_ALIAS_EXTRA_FIELD' must be a JSON object containing no fields except for ['aggregation', 'description', 'input signature', 'name', 'output signature', 'sql function', 'type']"
            ),
            id="BAD_FUNCTION_SQL_ALIAS_EXTRA_FIELD",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_WINDOW_ALIAS_MISSING_SQL_FUNCTION",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_WINDOW_ALIAS_MISSING_SQL_FUNCTION' must be a JSON object containing a field 'sql function' and field 'sql function' must be a string",
            id="BAD_FUNCTION_SQL_WINDOW_ALIAS_MISSING_SQL_FUNCTION",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_SQL_FUNCTION",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_SQL_FUNCTION' must be a JSON object containing a field 'sql function' and field 'sql function' must be a string",
            id="BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_SQL_FUNCTION",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_REQUIRES_ORDER",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_REQUIRES_ORDER' must be a JSON object containing a field 'requires order' and field 'requires order' must be a boolean",
            id="BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_REQUIRES_ORDER",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_ALLOWS_FRAME",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_ALLOWS_FRAME' must be a JSON object containing a field 'requires order' and field 'requires order' must be a boolean",
            id="BAD_FUNCTION_SQL_WINDOW_ALIAS_BAD_ALLOWS_FRAME",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_WINDOW_ALIAS_EXTRA_FIELD",
            re.escape(
                "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_SQL_WINDOW_ALIAS_EXTRA_FIELD' must be a JSON object containing no fields except for ['allows frame', 'description', 'input signature', 'name', 'output signature', 'requires order', 'sql function', 'type']"
            ),
            id="BAD_FUNCTION_SQL_WINDOW_ALIAS_EXTRA_FIELD",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_MACRO_MISSING_MACRO_TEXT",
            "metadata for definition of UDF 'POSITIVE' within graph 'BAD_FUNCTION_SQL_MACRO_MISSING_MACRO_TEXT' must be a JSON object containing a field 'macro text' and field 'macro text' must be a string",
            id="BAD_FUNCTION_SQL_MACRO_MISSING_MACRO_TEXT",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_MACRO_BAD_MACRO_TEXT",
            "metadata for definition of UDF 'POSITIVE' within graph 'BAD_FUNCTION_SQL_MACRO_BAD_MACRO_TEXT' must be a JSON object containing a field 'macro text' and field 'macro text' must be a string",
            id="BAD_FUNCTION_SQL_MACRO_BAD_MACRO_TEXT",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_MACRO_BAD_AGGREGATION",
            "metadata for definition of UDF 'POSITIVE' within graph 'BAD_FUNCTION_SQL_MACRO_BAD_AGGREGATION' must be a JSON object containing a field 'aggregation' and field 'aggregation' must be a boolean",
            id="BAD_FUNCTION_SQL_MACRO_BAD_AGGREGATION",
        ),
        pytest.param(
            "BAD_FUNCTION_SQL_MACRO_EXTRA_FIELD",
            re.escape(
                "metadata for definition of UDF 'POSITIVE' within graph 'BAD_FUNCTION_SQL_MACRO_EXTRA_FIELD' must be a JSON object containing no fields except for ['aggregation', 'description', 'input signature', 'macro text', 'name', 'output signature', 'type']"
            ),
            id="BAD_FUNCTION_SQL_MACRO_EXTRA_FIELD",
        ),
        pytest.param(
            "BAD_FUNCTION_VERIFIER_TYPE",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_VERIFIER_TYPE' must be a JSON object containing a field 'input signature' and field 'input signature' must be a JSON object",
            id="BAD_FUNCTION_VERIFIER_TYPE",
        ),
        pytest.param(
            "BAD_FUNCTION_VERIFIER_MISSING_TYPE",
            "verifier JSON metadata must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_FUNCTION_VERIFIER_MISSING_TYPE",
        ),
        pytest.param(
            "BAD_FUNCTION_VERIFIER_BAD_TYPE_1",
            "verifier JSON metadata must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_FUNCTION_VERIFIER_BAD_TYPE_1",
        ),
        pytest.param(
            "BAD_FUNCTION_VERIFIER_BAD_TYPE_2",
            "Unknown verifier type string: 'hello world'",
            id="BAD_FUNCTION_VERIFIER_BAD_TYPE_2",
        ),
        pytest.param(
            "BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_MISSING_VALUE",
            "fixed arguments verifier JSON data must be a JSON object containing a field 'value' and field 'value' must be a JSON array",
            id="BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_MISSING_VALUE",
        ),
        pytest.param(
            "BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_BAD_VALUE_1",
            "fixed arguments verifier JSON data must be a JSON object containing a field 'value' and field 'value' must be a JSON array",
            id="BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_BAD_VALUE_1",
        ),
        pytest.param(
            "BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_BAD_VALUE_2",
            "Invalid type value in fixed arguments verifier JSON data: 0",
            id="BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_BAD_VALUE_2",
        ),
        pytest.param(
            "BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_BAD_VALUE_3",
            "Unrecognized type string 'foo'",
            id="BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_BAD_VALUE_3",
        ),
        pytest.param(
            "BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_EXTRA_FIELD",
            re.escape(
                "fixed arguments verifier JSON metadata must be a JSON object containing no fields except for ['type', 'value']"
            ),
            id="BAD_FUNCTION_FIXED_ARGUMENTS_VERIFIER_EXTRA_FIELD",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_MISSING_VALUE",
            "argument range verifier JSON data must be a JSON object containing a field 'value' and field 'value' must be a JSON array",
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_MISSING_VALUE",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_VALUE_1",
            "argument range verifier JSON data must be a JSON object containing a field 'value' and field 'value' must be a JSON array",
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_VALUE_1",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_VALUE_2",
            re.escape(
                "Invalid type value in argument range verifier JSON data: ['numeric']"
            ),
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_VALUE_2",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_VALUE_3",
            "Unrecognized type string 'idk'",
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_VALUE_3",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_MISSING_MIN",
            "argument range verifier JSON data must be a JSON object containing a field 'min' and field 'min' must be a integer",
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_MISSING_MIN",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_MIN_1",
            "argument range verifier JSON data must be a JSON object containing a field 'min' and field 'min' must be a integer",
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_MIN_1",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_MIN_2",
            "Invalid minimum argument count in argument range verifier JSON data: -1",
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_MIN_2",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_MIN_3",
            "Invalid argument range verifier JSON data: minimum 3 is greater than the number of types provided: 2",
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_BAD_MIN_3",
        ),
        pytest.param(
            "BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_EXTRA_FIELD",
            re.escape(
                "argument range verifier JSON metadata must be a JSON object containing no fields except for ['min', 'type', 'value']"
            ),
            id="BAD_FUNCTION_ARGUMENT_RANGE_VERIFIER_EXTRA_FIELD",
        ),
        pytest.param(
            "BAD_FUNCTION_DEDUCER_TYPE",
            "metadata for definition of UDF 'FORMAT_DATETIME' within graph 'BAD_FUNCTION_DEDUCER_TYPE' must be a JSON object containing a field 'output signature' and field 'output signature' must be a JSON object",
            id="BAD_FUNCTION_DEDUCER_TYPE",
        ),
        pytest.param(
            "BAD_FUNCTION_DEDUCER_MISSING_TYPE",
            "deducer JSON metadata must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_FUNCTION_DEDUCER_MISSING_TYPE",
        ),
        pytest.param(
            "BAD_FUNCTION_DEDUCER_BAD_TYPE_1",
            "deducer JSON metadata must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_FUNCTION_DEDUCER_BAD_TYPE_1",
        ),
        pytest.param(
            "BAD_FUNCTION_DEDUCER_BAD_TYPE_2",
            "Unknown deducer type: 'buzz'",
            id="BAD_FUNCTION_DEDUCER_BAD_TYPE_2",
        ),
        pytest.param(
            "BAD_FUNCTION_CONSTANT_DEDUCER_MISSING_VALUE",
            "constant deducer JSON data must be a JSON object containing a field 'value' and field 'value' must be a string",
            id="BAD_FUNCTION_CONSTANT_DEDUCER_MISSING_VALUE",
        ),
        pytest.param(
            "BAD_FUNCTION_CONSTANT_DEDUCER_BAD_VALUE_1",
            "constant deducer JSON data must be a JSON object containing a field 'value' and field 'value' must be a string",
            id="BAD_FUNCTION_CONSTANT_DEDUCER_BAD_VALUE_1",
        ),
        pytest.param(
            "BAD_FUNCTION_CONSTANT_DEDUCER_BAD_VALUE_2",
            "Unrecognized type string 'bad-type'",
            id="BAD_FUNCTION_CONSTANT_DEDUCER_BAD_VALUE_2",
        ),
        pytest.param(
            "BAD_FUNCTION_CONSTANT_DEDUCER_EXTRA_FIELD",
            re.escape(
                "constant deducer JSON metadata must be a JSON object containing no fields except for ['type', 'value']"
            ),
            id="BAD_FUNCTION_CONSTANT_DEDUCER_EXTRA_FIELD",
        ),
        pytest.param(
            "BAD_FUNCTION_SELECT_ARGUMENT_DEDUCER_MISSING_VALUE",
            "select argument deducer JSON data must be a JSON object containing a field 'value' and field 'value' must be a integer",
            id="BAD_FUNCTION_SELECT_ARGUMENT_DEDUCER_MISSING_VALUE",
        ),
        pytest.param(
            "BAD_FUNCTION_SELECT_ARGUMENT_DEDUCER_BAD_VALUE_1",
            "select argument deducer JSON data must be a JSON object containing a field 'value' and field 'value' must be a integer",
            id="BAD_FUNCTION_SELECT_ARGUMENT_DEDUCER_BAD_VALUE_1",
        ),
        pytest.param(
            "BAD_FUNCTION_SELECT_ARGUMENT_DEDUCER_BAD_VALUE_2",
            "Invalid argument index in select argument deducer JSON data: -1",
            id="BAD_FUNCTION_SELECT_ARGUMENT_DEDUCER_BAD_VALUE_2",
        ),
        pytest.param(
            "BAD_FUNCTION_SELECT_ARGUMENT_DEDUCER_EXTRA_FIELD",
            re.escape(
                "select argument deducer JSON metadata must be a JSON object containing no fields except for ['type', 'value']"
            ),
            id="BAD_FUNCTION_SELECT_ARGUMENT_DEDUCER_EXTRA_FIELD",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_MISSING_COLUMN_NAME",
            "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_MISSING_COLUMN_NAME' must be a JSON object containing a field 'column name' and field 'column name' must be a string",
            id="MASKED_TABLE_COLUMN_MISSING_COLUMN_NAME",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_BAD_COLUMN_NAME",
            "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_BAD_COLUMN_NAME' must be a JSON object containing a field 'column name' and field 'column name' must be a string",
            id="MASKED_TABLE_COLUMN_BAD_COLUMN_NAME",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_MISSING_DATA_TYPE",
            "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_MISSING_DATA_TYPE' must be a JSON object containing a field 'data type' and field 'data type' must be a string",
            id="MASKED_TABLE_COLUMN_MISSING_DATA_TYPE",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_BAD_DATA_TYPE",
            "Unrecognized type string 'strdatmeric'",
            id="MASKED_TABLE_COLUMN_BAD_DATA_TYPE",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_BAD_PROTECTED_DATA_TYPE",
            "Unrecognized type string 'num'",
            id="MASKED_TABLE_COLUMN_BAD_PROTECTED_DATA_TYPE",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_MISSING_UNPROTECT_PROTOCOL",
            "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_MISSING_UNPROTECT_PROTOCOL' must be a JSON object containing a field 'unprotect protocol' and field 'unprotect protocol' must be a string",
            id="MASKED_TABLE_COLUMN_MISSING_UNPROTECT_PROTOCOL",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_BAD_UNPROTECT_PROTOCOL",
            "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_BAD_UNPROTECT_PROTOCOL' must be a JSON object containing a field 'unprotect protocol' and field 'unprotect protocol' must be a string",
            id="MASKED_TABLE_COLUMN_BAD_UNPROTECT_PROTOCOL",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_MISSING_PROTECT_PROTOCOL",
            "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_MISSING_PROTECT_PROTOCOL' must be a JSON object containing a field 'protect protocol' and field 'protect protocol' must be a string",
            id="MASKED_TABLE_COLUMN_MISSING_PROTECT_PROTOCOL",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_BAD_PROTECT_PROTOCOL",
            "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_BAD_PROTECT_PROTOCOL' must be a JSON object containing a field 'protect protocol' and field 'protect protocol' must be a string",
            id="MASKED_TABLE_COLUMN_BAD_PROTECT_PROTOCOL",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_BAD_SERVER_MASKED",
            "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_BAD_SERVER_MASKED' must be a JSON object containing a field 'server masked' and field 'server masked' must be a boolean",
            id="MASKED_TABLE_COLUMN_BAD_SERVER_MASKED",
        ),
        pytest.param(
            "MASKED_TABLE_COLUMN_EXTRA_FIELDS",
            re.escape(
                "masked table column property 'name' of simple table collection 'customers' in graph 'MASKED_TABLE_COLUMN_EXTRA_FIELDS' must be a JSON object containing no fields except for ['column name', 'data type', 'description', 'extra semantic info', 'name', 'protect protocol', 'protected data type', 'sample values', 'server masked', 'synonyms', 'type', 'unprotect protocol']"
            ),
            id="MASKED_TABLE_COLUMN_EXTRA_FIELDS",
        ),
    ],
)
def test_invalid_graphs(
    invalid_graph_path: str, graph_name: str, error_message: str
) -> None:
    with pytest.raises(
        (PyDoughMetadataException, PyDoughTypeException), match=error_message
    ):
        parse_json_metadata_from_file(
            file_path=invalid_graph_path, graph_name=graph_name
        )


@pytest.mark.parametrize(
    "pydough_string, error_message",
    [
        pytest.param(
            "parent.sub1",
            "Malformed general join condition: '' (Expected the join condition to be a valid PyDough expression)",
            id="empty",
        ),
        pytest.param(
            "parent.sub2",
            "Malformed general join condition: '(4' ('(' was never closed (<unknown>, line 1))",
            id="bad_syntax_1",
        ),
        pytest.param(
            "parent.sub3",
            "Malformed general join condition: 'self.j1 in self.k2' (PyDough objects cannot be used with the 'in' operator.)",
            id="bad_syntax_2",
        ),
        pytest.param(
            "parent.sub4",
            "Malformed general join condition: 'is_prime(self.j1) != is_prime(self.j2)' (PyDough object is_prime is not callable. Did you mean: ISIN, LIKE, SUM, SLICE, STRIP, IFF, MIN, VAR, PREV, SIGN, SQRT, STRING, ABS, CEIL, FIND, HAS, HOUR, LPAD, RPAD, STD, YEAR, UPPER, DATETIME?)",
            id="bad_syntax_3",
        ),
        pytest.param(
            "parent.sub5",
            "Malformed general join condition: 'foo = other.k1' (invalid syntax. Maybe you meant '==' or ':=' instead of '='? (<unknown>, line 1))",
            id="bad_syntax_4",
        ),
        pytest.param(
            "parent.sub6",
            "Malformed general join condition: 'self.j1 == COUNT(self.sub0)' (Accessing sub-collection terms is currently unsupported in PyDough general join conditions)",
            id="collection_1",
        ),
        pytest.param(
            "parent.sub7",
            "Malformed general join condition: 'self.j1 == other.sub0.k1' (Accessing sub-collection terms is currently unsupported in PyDough general join conditions)",
            id="collection_2",
        ),
        pytest.param(
            "parent.sub8",
            "Malformed general join condition: 'self.sub0.CALCULATE(y=YEAR(k1)) == other.k1' (Collection accesses are currently unsupported in PyDough general join conditions)",
            id="collection_3",
        ),
        pytest.param(
            "parent.sub9",
            "Malformed general join condition: 'RANKING(by=self.j1.ASC()) < other.k1' (Window functions are currently unsupported in PyDough general join conditions)",
            id="window_function",
        ),
        pytest.param(
            "parent.sub10",
            "Malformed general join condition: 'self.j1 == other.k5' (Unrecognized term of BAD_JOIN_CONDITIONS.parent.sub10: 'k5'. Did you mean: k1, k2, k3, k4?)",
            id="wrong_names_1",
        ),
        pytest.param(
            "parent.sub11",
            "Malformed general join condition: 'this.j1 == other.k1' (Accessing sub-collection terms is currently unsupported in PyDough general join conditions",
            id="wrong_names_2",
        ),
    ],
)
def test_invalid_general_join_conditions(
    invalid_graph_path: str,
    pydough_string: str,
    error_message: str,
    default_config: PyDoughConfigs,
) -> None:
    with pytest.raises(Exception, match=re.escape(error_message)):
        graph: GraphMetadata = parse_json_metadata_from_file(
            file_path=invalid_graph_path, graph_name="BAD_JOIN_CONDITIONS"
        )
        pydough_string = ast.unparse(
            transform_code(f"answer = {pydough_string}", {"graph": graph}, set())
        )
        local_variables: dict[str, object] = {"graph": graph}
        exec(pydough_string, {}, local_variables)
        pydough_code = local_variables["answer"]
        assert isinstance(pydough_code, UnqualifiedNode)
        qualify_node(pydough_code, graph, default_config)
