"""
TODO: add file-level docstring
"""

import pytest
import re
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
            "REGION_FORMAT_1",
            "metadata for PyDough graph must be a dict",
            id="graph-not_json",
        ),
        pytest.param(
            "REGION_FORMAT_2",
            "collection 'Regions' in graph 'REGION_FORMAT_2' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-not_json",
        ),
        pytest.param(
            "REGION_FORMAT_3",
            "property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_3' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="property-not_json",
        ),
        pytest.param(
            "REGION_FORMAT_4",
            "collection 'Regions' in graph 'REGION_FORMAT_4' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-collection_type-missing",
        ),
        pytest.param(
            "REGION_FORMAT_5",
            "collection 'Regions' in graph 'REGION_FORMAT_5' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="collection-collection_type-wrong_type",
        ),
        pytest.param(
            "REGION_FORMAT_6",
            "nrecognized collection type for property 'Regions' of graph 'REGION_FORMAT_6': '¡SIMPLE!'",
            id="collection-collection_type-bad_name",
        ),
        pytest.param(
            "REGION_FORMAT_7",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_7' must be a JSON object containing a field 'table_path' and field 'table_path' must be a string",
            id="simple_table-table_path-missing",
        ),
        pytest.param(
            "REGION_FORMAT_8",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_8' must be a JSON object containing a field 'table_path' and field 'table_path' must be a string",
            id="simple_table-table_path-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_9",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_9' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-missing",
        ),
        pytest.param(
            "REGION_FORMAT_10",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_10' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_11",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_11' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_12",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_12' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_13",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_13' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_14",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_14' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_15",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_15' must be a JSON object containing a field 'unique_properties' and field 'unique_properties' must be a non-empty list where each element must be a string or it must be a non-empty list where each element must be a string",
            id="simple_table-unique_properties-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_16",
            "property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_16' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="simple_table-property_type-missing",
        ),
        pytest.param(
            "REGION_FORMAT_17",
            "property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_17' must be a JSON object containing a field 'type' and field 'type' must be a string",
            id="simple_table-property_type-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_18",
            "Unrecognized property type for property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_18': 'table column'",
            id="simple_table-property_type-bad_name",
        ),
        pytest.param(
            "REGION_FORMAT_19",
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_19' must be a JSON object containing a field 'column_name' and field 'column_name' must be a string",
            id="table_column-column_name-missing",
        ),
        pytest.param(
            "REGION_FORMAT_20",
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_20' must be a JSON object containing a field 'column_name' and field 'column_name' must be a string",
            id="table_column-column_name-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_21",
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_21' must be a JSON object containing a field 'data_type' and field 'data_type' must be a string",
            id="table_column-data_type-missing",
        ),
        pytest.param(
            "REGION_FORMAT_22",
            "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_22' must be a JSON object containing a field 'data_type' and field 'data_type' must be a string",
            id="table_column-data_type-bad_type",
        ),
        pytest.param(
            "REGION_FORMAT_23",
            "Unrecognized type string 'int512'",
            id="table_column-data_type-bad_name",
        ),
        pytest.param(
            "REGION_FORMAT_24",
            re.escape(
                "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_24' must be a JSON object containing no fields except for ['column_name', 'data_type', 'type']"
            ),
            id="table_column-extra_field",
        ),
        pytest.param(
            "REGION_FORMAT_25",
            re.escape(
                "simple table collection 'Regions' in graph 'REGION_FORMAT_25' must be a JSON object containing no fields except for ['properties', 'table_path', 'type', 'unique_properties']"
            ),
            id="simple_table-extra_field",
        ),
        pytest.param(
            "REGION_FORMAT_26",
            "simple join property 'nations' of simple table collection 'Regions' in graph 'REGION_FORMAT_26' cannot be a unique property since it is a subcollection",
            id="simple_table-unique_property-is_subcollection",
        ),
        pytest.param(
            "REGION_FORMAT_27",
            "simple table collection 'Regions' in graph 'REGION_FORMAT_27' does not have a property named 'region_key' to use as a unique property",
            id="simple_table-unique_property-does_not_exist",
        ),
        pytest.param(
            "REGION_FORMAT_28",
            re.escape(
                "simple table collection 'Regions' in graph 'REGION_FORMAT_28' has malformed unique properties set: ['key', 'name', 'key']"
            ),
            id="simple_table-unique_property-duplicates",
        ),
        pytest.param(
            "REGION_FORMAT_29",
            re.escape(
                "simple table collection 'Regions' in graph 'REGION_FORMAT_29' has malformed unique properties set: [['key', 'name'], ['name', 'key']]"
            ),
            id="simple_table-unique_property-duplicates",
        ),
        pytest.param(
            "REGION_FORMAT_30",
            re.escape(
                "simple table collection 'Regions' in graph 'REGION_FORMAT_30' has malformed unique properties set: [['key', 'name', 'key']]"
            ),
            id="simple_table-unique_property-duplicates",
        ),
        pytest.param(
            "REGION_FORMAT_31",
            re.escape(
                "table column property 'key' of simple table collection 'Regions' in graph 'REGION_FORMAT_31' must be a JSON object containing no fields except for ['column_name', 'data_type', 'type']"
            ),
            id="simple_table-table_collection-extra_field",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_1",
            "cartesian property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_1' must be a JSON object containing a field 'other_collection_name' and field 'other_collection_name' must be a string",
            id="cartesian_product-other_collection_name-missing",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_2",
            "cartesian property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_2' must be a JSON object containing a field 'other_collection_name' and field 'other_collection_name' must be a string",
            id="cartesian_product-other_collection_name-bad_type",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_3",
            "graph 'PARTSUPP_FORMAT_3' must be a JSON object containing a field 'PartSupp' and field 'PartSupp' must be a CollectionMetadata",
            id="cartesian_product-other_collection_name-does_not_exist",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_4",
            "cartesian property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_4' must be a JSON object containing a field 'reverse_relationship_name' and field 'reverse_relationship_name' must be a string",
            id="cartesian_product-reverse_relationship_name-missing",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_5",
            "cartesian property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_5' must be a JSON object containing a field 'reverse_relationship_name' and field 'reverse_relationship_name' must be a string",
            id="cartesian_product-reverse_relationship_name-bad_type",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_6",
            "Duplicate property: table column property 'part_key' of simple table collection 'PartSupp' in graph 'PARTSUPP_FORMAT_6' versus cartesian property 'part_key' of simple table collection 'PartSupp' in graph 'PARTSUPP_FORMAT_6'.",
            id="cartesian_product-reverse_relationship_name-overload",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_7",
            re.escape(
                "cartesian property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_7' must be a JSON object containing no fields except for ['other_collection_name', 'reverse_relationship_name', 'type']"
            ),
            id="cartesian_product-extra_field",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_8",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_8' must be a JSON object containing a field 'other_collection_name' and field 'other_collection_name' must be a string",
            id="simple_join-other_collection_name-missing",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_9",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_9' must be a JSON object containing a field 'other_collection_name' and field 'other_collection_name' must be a string",
            id="simple_join-other_collection_name-bad_type",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_10",
            re.escape(
                "Unable to extract dependencies of properties in PyDough metadata due to unrecognized property ('PartSupp', 'part_key')"
            ),
            id="simple_join-other_collection_name-no_exist",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_11",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_11' must be a JSON object containing a field 'reverse_relationship_name' and field 'reverse_relationship_name' must be a string",
            id="simple_join-reverse_relationship_name-missing",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_12",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_12' must be a JSON object containing a field 'reverse_relationship_name' and field 'reverse_relationship_name' must be a string",
            id="simple_join-reverse_relationship_name-bad_type",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_13",
            "Duplicate property: table column property 'supplier_key' of simple table collection 'PartSupp' in graph 'PARTSUPP_FORMAT_13' versus simple join property 'supplier_key' of simple table collection 'PartSupp' in graph 'PARTSUPP_FORMAT_13'.",
            id="simple_join-reverse_relationship_name-overload",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_14",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_14' must be a JSON object containing a field 'singular' and field 'singular' must be a boolean",
            id="simple_join-singular-missing",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_15",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_15' must be a JSON object containing a field 'singular' and field 'singular' must be a boolean",
            id="simple_join-singular-bad_type",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_16",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_16' must be a JSON object containing a field 'no_collisions' and field 'no_collisions' must be a boolean",
            id="simple_join-no_collisions-missing",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_17",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_17' must be a JSON object containing a field 'no_collisions' and field 'no_collisions' must be a boolean",
            id="simple_join-no_collisions-bad_type",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_18",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_18' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="simple_join-keys-missing",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_19",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_19' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="simple_join-keys-bad_type",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_20",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_20' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="simple_join-keys-bad_type",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_21",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_21' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="simple_join-keys-empty_outer",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_22",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_22' must be a JSON object containing a field 'keys' and field 'keys' must be a non-empty dictionary where each key must be a string and each value must be a non-empty list where each element must be a string",
            id="simple_join-keys-empty_inner",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_23",
            re.escape(
                "Unable to extract dependencies of properties in PyDough metadata due to unrecognized property ('Parts', 'fizz')"
            ),
            id="simple_join-keys-absent_key",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_24",
            re.escape(
                "Unable to extract dependencies of properties in PyDough metadata due to unrecognized property ('PartSupp', 'buzz')"
            ),
            id="simple_join-keys-absent_other_key",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_25",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_25' cannot use cartesian property 'cartesian_supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_25' as a join key",
            id="simple_join-keys-key_not_scalar",
        ),
        pytest.param(
            "PARTSUPP_FORMAT_26",
            "simple join property 'supply_records' of simple table collection 'Parts' in graph 'PARTSUPP_FORMAT_26' cannot use cartesian property 'cartesian_parts_records' of simple table collection 'PartSupp' in graph 'PARTSUPP_FORMAT_26' as a join key",
            id="simple_join-keys-other_key_not_scalar",
        ),
    ],
)
def test_invalid_graphs(invalid_graph_path, graph_name, error_message):
    with pytest.raises(PyDoughMetadataException, match=error_message):
        parse_json_metadata_from_file(
            file_path=invalid_graph_path, graph_name=graph_name
        )
