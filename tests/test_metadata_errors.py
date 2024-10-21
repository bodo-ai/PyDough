"""
TODO: add file-level docstring
"""

import pytest
from pydough.metadata.errors import PyDoughMetadataException
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.collections import CollectionMetadata
from pydough import parse_json_metadata_from_file


def test_missing_collection(get_sample_graph):
    """
    Testing the error handling for trying to fetch a collection that does not
    exist.
    """
    graph: GraphMetadata = get_sample_graph("tpch")
    with pytest.raises(
        PyDoughMetadataException,
        match="graph 'TPCH' does not have a collection named 'Inventory'",
    ):
        graph.get_collection("Inventory")


def test_missing_property(get_sample_graph):
    """
    Testing the error handling for trying to fetch a collection that does not
    exist.
    """
    graph: GraphMetadata = get_sample_graph("tpch")
    with pytest.raises(
        PyDoughMetadataException,
        match="simple table collection 'Parts' in graph 'TPCH' does not have a property 'color'",
    ):
        collection: CollectionMetadata = graph.get_collection("Parts")
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
            "#badgraphname",
            "graph name must be a string that is a Python identifier",
            id="naming-invalid_collection",
        ),
        pytest.param(
            "BAD_NAME_1",
            "collection name must be a string that is a Python identifier",
            id="naming-invalid_property",
        ),
        pytest.param(
            "BAD_NAME_2",
            "property name must be a string that is a Python identifier",
            id="naming-invalid_property",
        ),
        pytest.param(
            "BAD_NAME_3",
            "Cannot have collection named 'BAD_NAME_3' share the same name as the graph containing it.",
            id="naming-collection_same_as_graph",
        ),
        pytest.param(
            "FORMAT_1", "metadata for PyDough graph must be a dict", id="graph-not_json"
        ),
        pytest.param(
            "FORMAT_2",
            "collection 'BAD_COLLECTION' in graph 'FORMAT_2' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-not_json",
        ),
        pytest.param(
            "FORMAT_3",
            "property 'key' of collection simple table collection 'BAD_COLLECTION' in graph 'FORMAT_3' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="property-not_json",
        ),
        pytest.param(
            "FORMAT_4",
            "collection 'BAD_COLLECTION' in graph 'FORMAT_4' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-collection_type-missing",
        ),
        pytest.param(
            "FORMAT_5",
            "collection 'BAD_COLLECTION' in graph 'FORMAT_5' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-collection_type-wrong_type",
        ),
        pytest.param(
            "FORMAT_6",
            "Unrecognized collection type: '¡SIMPLE!",
            id="collection-collection_type-bad_name",
        ),
        pytest.param(
            "FORMAT_7",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_7' must be a JSON object containing a field 'table_path' and field 'table_path' must be a string",
            id="simple_table-table_path-missing",
        ),
        pytest.param(
            "FORMAT_8",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_8' must be a JSON object containing a field 'table_path' and field 'table_path' must be a string",
            id="simple_table-table_path-bad_type",
        ),
        pytest.param(
            "FORMAT_9",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_9' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-missing",
        ),
        pytest.param(
            "FORMAT_10",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_10' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "FORMAT_11",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_11' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "FORMAT_12",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_12' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "FORMAT_13",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_13' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "FORMAT_14",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_14' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "FORMAT_15",
            "simple table collection 'BAD_COLLECTION' in graph 'FORMAT_15' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
    ],
)
def test_invalid_graphs(invalid_graph_path, graph_name, error_message):
    with pytest.raises(PyDoughMetadataException, match=error_message):
        parse_json_metadata_from_file(
            file_path=invalid_graph_path, graph_name=graph_name
        )
