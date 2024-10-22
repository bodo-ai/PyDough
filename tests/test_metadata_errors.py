"""
TODO: add file-level docstring
"""

import pytest
from pydough.metadata.errors import PyDoughMetadataException
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.collections import CollectionMetadata
from pydough import parse_json_metadata_from_file
from pydough.types.errors import PyDoughTypeException


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
    "graph_name, exception_type, error_message",
    [
        pytest.param(
            "NO_EXIST",
            PyDoughMetadataException,
            "PyDough metadata file located at '(.+)' does not contain a graph named 'NO_EXIST'",
            id="missing_graph",
        ),
        pytest.param(
            "#badgraphname",
            PyDoughMetadataException,
            "graph name must be a string that is a Python identifier",
            id="naming-invalid_collection",
        ),
        pytest.param(
            "BAD_NAME_1",
            PyDoughMetadataException,
            "collection name must be a string that is a Python identifier",
            id="naming-invalid_property",
        ),
        pytest.param(
            "BAD_NAME_2",
            PyDoughMetadataException,
            "property name must be a string that is a Python identifier",
            id="naming-invalid_property",
        ),
        pytest.param(
            "BAD_NAME_3",
            PyDoughMetadataException,
            "Cannot have collection named 'BAD_NAME_3' share the same name as the graph containing it.",
            id="naming-collection_same_as_graph",
        ),
        pytest.param(
            "REGION_FORMAT_1",
            PyDoughMetadataException,
            "metadata for PyDough graph must be a dict",
            id="graph-not_json",
        ),
        pytest.param(
            "REGION_FORMAT_2",
            PyDoughMetadataException,
            "collection 'Regions' in graph 'REGION_FORMAT_2' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-not_json",
        ),
        pytest.param(
            "REGION_FORMAT_3",
            PyDoughMetadataException,
            "property 'key' of collection simple table collection 'Regions' in graph 'REGION_FORMAT_3' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="property-not_json",
        ),
        pytest.param(
            "REGION_FORMAT_4",
            PyDoughMetadataException,
            "collection 'Regions' in graph 'REGION_FORMAT_4' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-collection_type-missing",
        ),
        pytest.param(
            "REGION_FORMAT_5",
            PyDoughMetadataException,
            "collection 'Regions' in graph 'REGION_FORMAT_5' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-collection_type-wrong_type",
        ),
        pytest.param(
            "REGION_FORMAT_6",
            PyDoughMetadataException,
            "Unrecognized collection type: '¡SIMPLE!",
            id="collection-collection_type-bad_name",
        ),
        pytest.param(
            "REGION_FORMAT_7",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_7' must be a JSON object containing a field 'table_path' and field 'table_path' must be a string",
            id="simple_table-table_path-missing",
        ),
        pytest.param(
            "REGION_FORMAT_8",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_8' must be a JSON object containing a field 'table_path' and field 'table_path' must be a string",
            id="simple_table-table_path-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_9",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_9' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-missing",
        ),
        pytest.param(
            "REGION_FORMAT_10",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_10' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_11",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_11' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_12",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_12' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_13",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_13' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_14",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_14' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_15",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_15' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_16",
            PyDoughMetadataException,
            "property 'key' of collection simple table collection 'Regions' in graph 'REGION_FORMAT_16' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="simple_table-property_type-missing",
        ),
        pytest.param(
            "REGION_FORMAT_17",
            PyDoughMetadataException,
            "property 'key' of collection simple table collection 'Regions' in graph 'REGION_FORMAT_17' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="simple_table-property_type-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_18",
            PyDoughMetadataException,
            "Unrecognized property type for property 'key' of collection simple table collection 'Regions' in graph 'REGION_FORMAT_18': 'table column'",
            id="simple_table-property_type-bad_name",
        ),
        pytest.param(
            "REGION_FORMAT_19",
            PyDoughMetadataException,
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_19' must be a JSON object containing a field 'column_name' and field 'column_name' must be a string",
            id="simple_table-table_column-column_name-missing",
        ),
        pytest.param(
            "REGION_FORMAT_20",
            PyDoughMetadataException,
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_20' must be a JSON object containing a field 'column_name' and field 'column_name' must be a string",
            id="simple_table-table_column-column_name-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_21",
            PyDoughMetadataException,
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_21' must be a JSON object containing a field 'data_type' and field 'data_type' must be a string",
            id="simple_table-table_column-data_type-missing",
        ),
        pytest.param(
            "REGION_FORMAT_22",
            PyDoughMetadataException,
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_22' must be a JSON object containing a field 'data_type' and field 'data_type' must be a string",
            id="simple_table-table_column-data_type-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_23",
            PyDoughTypeException,
            "Unrecognized type string 'int512'",
            id="simple_table-table_column-data_type-bad_name",
        ),
        pytest.param(
            "REGION_FORMAT_24",
            PyDoughMetadataException,
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_24' must be a JSON object containing no fields except for \\['column_name', 'data_type', 'type'\\]",
            id="simple_table-table_column-extra_field",
        ),
        pytest.param(
            "REGION_FORMAT_25",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_25' must be a JSON object containing no fields except for \\['properties', 'table_path', 'type', 'unique_properties'\\]",
            id="simple_table-extra_field",
        ),
        pytest.param(
            "REGION_FORMAT_26",
            PyDoughMetadataException,
            "simple join property 'nations' of simple table collection 'Regions' in graph 'REGION_FORMAT_26' cannot be a unique property since it is a subcollection",
            id="simple_table-unique_property-is_subcollection",
        ),
        pytest.param(
            "REGION_FORMAT_27",
            PyDoughMetadataException,
            "simple table collection 'Regions' in graph 'REGION_FORMAT_27' does not have a property named 'region_key' to use as a unique property",
            id="simple_table-unique_property-does_not_exist",
        ),
    ],
)
def test_invalid_graphs(invalid_graph_path, graph_name, exception_type, error_message):
    with pytest.raises(exception_type, match=error_message):
        parse_json_metadata_from_file(
            file_path=invalid_graph_path, graph_name=graph_name
        )
