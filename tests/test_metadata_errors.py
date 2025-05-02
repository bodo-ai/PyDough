"""
Error-handling unit tests for the PyDough metadata module.
"""

import ast
import re

import pytest
from test_utils import graph_fetcher

from pydough import parse_json_metadata_from_file
from pydough.configs import PyDoughConfigs
from pydough.metadata import CollectionMetadata, GraphMetadata, PyDoughMetadataException
from pydough.unqualified import UnqualifiedNode, qualify_node, transform_code


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
                "graph 'EXTRA_GRAPH_FIELDS' must be a JSON object containing no fields except for ['additional definitions', 'collections', 'extra semantic info', 'name', 'relationships', 'verified pydough analysis', 'version']"
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
            "property 'property' of simple table collection 'collection' in graph 'MISSING_PROPERTIES_TYPE' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="MISSING_PROPERTIES_TYPE",
        ),
        pytest.param(
            "BAD_PROPERTIES_TYPE_1",
            "property 'property' of simple table collection 'collection' in graph 'BAD_PROPERTIES_TYPE_1' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="BAD_PROPERTIES_TYPE_1",
        ),
        pytest.param(
            "BAD_PROPERTIES_TYPE_2",
            "Unrecognized property type 'unknown property type' for property 'property' of simple table collection 'collection' in graph 'BAD_PROPERTIES_TYPE_2'",
            id="BAD_PROPERTIES_TYPE_2",
        ),
        pytest.param(
            "MISSING_TABLE_COLUMN_DATA_TYPE",
            "table column property 'property' of simple table collection 'collection' in graph 'MISSING_TABLE_COLUMN_DATA_TYPE' must be a JSON object containing a field 'data type' and field 'data type' must be a string",
            id="MISSING_TABLE_COLUMN_DATA_TYPE",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_DATA_TYPE_1",
            "table column property 'property' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_DATA_TYPE_1' must be a JSON object containing a field 'data type' and field 'data type' must be a string",
            id="BAD_TABLE_COLUMN_DATA_TYPE_1",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_DATA_TYPE_2",
            "Unrecognized type string 'foobar'",
            id="BAD_TABLE_COLUMN_DATA_TYPE_2",
        ),
        pytest.param(
            "MISSING_TABLE_COLUMN_COLUMN_NAME",
            "table column property 'property' of simple table collection 'collection' in graph 'MISSING_TABLE_COLUMN_COLUMN_NAME' must be a JSON object containing a field 'column name' and field 'column name' must be a string",
            id="MISSING_TABLE_COLUMN_COLUMN_NAME",
        ),
        pytest.param(
            "BAD_TABLE_COLUMN_COLUMN_NAME",
            "table column property 'property' of simple table collection 'collection' in graph 'BAD_TABLE_COLUMN_COLUMN_NAME' must be a JSON object containing a field 'column name' and field 'column name' must be a string",
            id="BAD_TABLE_COLUMN_COLUMN_NAME",
        ),
    ],
)
def test_invalid_graphs(
    invalid_graph_path: str, graph_name: str, error_message: str
) -> None:
    with pytest.raises(PyDoughMetadataException, match=error_message):
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
            "Malformed general join condition: 'is_prime(self.j1) != is_prime(self.j2)' (PyDough nodes is_prime is not callable. Did you mean to use a function?)",
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
            "Malformed general join condition: 'self.j1 == other.k5' (Unrecognized term of simple table collection 'child' in graph 'BAD_JOIN_CONDITIONS': 'k5')",
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
