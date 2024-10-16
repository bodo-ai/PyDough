"""
TODO: add file-level docstring
"""

from typing import Dict, List, Tuple, Set
from .graphs import GraphMetadata
from .errors import (
    PyDoughMetadataException,
    verify_json_has_property_with_type,
    verify_has_type,
)
from collections import deque
from .collections import CollectionMetadata
from .properties import PropertyMetadata
import json


def parse_json_metadata(file_path: str, graph_name: str) -> GraphMetadata:
    """
    TODO: add function docstring.
    """
    with open(file_path, "r") as f:
        as_json = json.load(f)
    if not isinstance(as_json, dict):
        raise PyDoughMetadataException(
            "PyDough metadata expected to be a JSON file containing a JSON object."
        )
    if graph_name not in as_json:
        raise PyDoughMetadataException(
            f"PyDough metadata does not contain a graph named {graph_name!r}"
        )
    graph_json = as_json[graph_name]
    return parse_graph(graph_name, graph_json)


def parse_graph(graph_name: str, graph_json: Dict) -> None:
    """
    TODO: add function docstring.
    """
    verify_has_type(graph_json, dict, "metadata for PyDough graph")
    graph = GraphMetadata(graph_name)

    # A list that will store each collection property in the metadata
    # before it is defined and added to its collection, so all of the properties
    # can be sorted based on their dependencies. The list stores the properties
    # as tuples in the form (collection_name, property_name, property_json)
    raw_properties: List[Tuple[str, str, dict]] = []

    # Iterate through all the key-value pairs in the graph to set up the
    # corresponding collections as empty metadata that will later be filled
    # with properties, and also obtain each of the properties.
    for collection_name in graph_json:
        # Add the raw collection metadata to the collections dictionary
        collection_json = graph_json[collection_name]
        CollectionMetadata.parse_from_json(graph, collection_name, collection_json)

        # Add the unprocessed properties of each collection to the properties list
        # (the parsing of the collection verified that the 'properties' key exists)
        properties_json = graph_json[collection_name]["properties"]
        for property_name in properties_json:
            property_json = properties_json[property_name]
            raw_properties.append((collection_name, property_name, property_json))

    ordered_properties = topologically_sort_properties(raw_properties)
    for collection_name, property_name, property_json in ordered_properties:
        verify_json_has_property_with_type(
            graph.collections, collection_name, CollectionMetadata, graph.error_name
        )
        collection = graph.collections[collection_name]
        PropertyMetadata.parse_from_json(collection, property_name, property_json)

    return graph


def topological_ordering(dependencies: List[List[int]]) -> List[int]:
    """
    TODO: add function docstring
    """
    valid_range = range(len(dependencies))
    finish_times = [-1 for _ in valid_range]
    visited = set()
    current_time = 0

    def dfs(idx, parent=-1):
        nonlocal current_time
        if idx in visited:
            return
        visited.add(idx)
        for neighbor in dependencies[idx]:
            if neighbor not in valid_range:
                raise PyDoughMetadataException(
                    "Malformed property dependencies detected."
                )
            if neighbor != parent and neighbor in visited:
                raise PyDoughMetadataException(
                    "Cyclic dependency detected between properties in PyDough metadata graph."
                )
            dfs(neighbor, idx)
        finish_times[idx] = current_time
        current_time += 1

    for idx in valid_range:
        dfs(idx)
    return finish_times


def get_property_dependencies(
    reformatted_properties: Dict[Tuple[str, str], Tuple[dict, int]],
) -> List[Set[int]]:
    """
    TODO: add function docstring
    """
    n_properties = len(reformatted_properties)
    if n_properties == 0:
        return []

    valid_range = range(n_properties)
    dependencies: List[Set[int]] = [set() for i in valid_range]
    defined = set()

    compound_names: Set[Tuple[str, str]] = set()
    compound_stack: deque = deque()
    reverses: Dict[Tuple[str, str], Tuple[str, str]] = {}
    collections_mapped_to: Dict[Tuple[str, str], str] = {}
    compound_inherited_aliases: Dict[Tuple[str, str], Set[str]] = {}
    for property in reformatted_properties:
        json, idx = reformatted_properties[property]
        match json["type"]:
            case "table_column":
                defined.add(property)
                iters_since_change = 0
            case "simple_join" | "cartesian_product":
                reverse_collection = json["other_collection_name"]
                reverse_property = json["reverse_relationship_name"]
                reverse = (reverse_collection, reverse_property)
                reverses[reverse_collection, reverse_property] = reverse
                collections_mapped_to[property] = reverse_collection
                collections_mapped_to[reverse_property] = property[0]
                defined.add(property)
                defined.add(reverse)
                iters_since_change = 0
            case "compound":
                compound_stack.append(property)
                compound_names.add(property)
            case typ:
                raise PyDoughMetadataException(
                    f"Unrecognized PyDough collection type: {typ!r}"
                )

    """
    Defining every collection mapped to + every reverse name

    For each compound property:
    - If its primary is defined (including as a reverse), identify the middle collection via collections_mapped_to
        - Declare the primary property as a dependency of the compound
        - If the secondary is defined (including as a reverse), identify the target collection via collections_mapped_to
            - Declare the secondary property as a dependency of the compound
            - Define the reverse of the property, and the collection it maps to, and the collection that the reverse maps to
            - For each inherited property:
                - If it has been defined as a property (including as a reverse) of the middle collection:
                    - Declare the inherited property as a dependency of the compound
                - If it is not defined but is known, push to the stack & move on
                - If it is not defined but is unknown and the primary property is a compound and the name matches one of its inherited properties: ignore
                - If it is not defined but is unknown and the primary property is not a compound: move compound back to the bottom of the stack and move on
            - If we have gotten this far and all the inherited properties were known, declare the compound defined
        - If the secondary is not defined but is known, push to the stack & move on
        - If the secondary is not defined but is unknown (possibly a reverse), move compound back to the bottom of the stack
    - If the primary is not defined but is known, push to the stack & move on
    - If the primary is not defined but is unknown (possibly a reverse), move compound back to the bottom of the stack

    Have an iteration check to verify that we do not have the same things being moved to the bottom of the stack over & over
    """

    def get_true_property(property: Tuple[str, str]) -> Tuple[str, str] | None:
        if property in reformatted_properties:
            return property
        if property in reverses:
            reverse = reverses[property]
            if reverse in reformatted_properties:
                return reverse
        return None

    def add_dependency(property: Tuple[str, str], dependency: Tuple[str, str]) -> None:
        true_property = get_true_property(property)
        true_dependency = get_true_property(dependency)
        if (
            true_property is None
            or true_dependency
            or true_property not in reformatted_properties
            or true_dependency not in reformatted_properties
        ):
            raise PyDoughMetadataException(
                "Unable to extract dependencies of properties in PyDough metadata"
            )
        property_idx = reformatted_properties[true_property][1]
        dependency_idx = reformatted_properties[true_dependency][1]
        dependencies[property_idx].add(dependency_idx)

    iters_since_change: int = 0
    while len(compound_stack) > 0:
        if iters_since_change > len(compound_stack):
            raise PyDoughMetadataException(
                "Unable to extract dependencies of properties in PyDough metadata"
            )
        property: Tuple[str, str] = compound_stack.pop()
        if property in defined:
            iters_since_change = 0
            continue
        json, _ = reformatted_properties[property]
        primary_property_name: str = json["primary_property"]
        original_collection: str = property[0]
        primary_property: Tuple[str, str] = (original_collection, primary_property_name)

        true_primary = get_true_property(primary_property)
        if true_primary is None:
            compound_stack.appendleft(property)
            continue

        if true_primary not in defined:
            compound_stack.append(property)
            compound_stack.append(true_primary)
            iters_since_change = 0
            continue

        add_dependency(property, true_primary)

        middle_collection: str = collections_mapped_to[primary_property]
        secondary_property_name: str = json["secondary_property"]
        secondary_property: Tuple[str, str] = (
            middle_collection,
            secondary_property_name,
        )

        true_secondary = get_true_property(property)
        if true_secondary is None:
            compound_stack.appendleft(property)
            continue

        if true_secondary not in defined:
            compound_stack.append(property)
            compound_stack.append(true_secondary)
            iters_since_change = 0
            continue

        add_dependency(property, true_secondary)

        target_collection: str = collections_mapped_to[secondary_property]
        reverse_property_name = json["reverse_relationship_name"]
        reverse_property = (target_collection, reverse_property_name)
        collections_mapped_to[property] = target_collection
        collections_mapped_to[reverse_property] = original_collection
        reverses[property] = reverse_property
        reverses[reverse_property] = property
        compound_names.add(reverse_property)

        inherited_properties: Dict[str, str] = json["inherited_properties"].values()
        new_dependencies = []
        must_restart = False
        done = True
        for alias, inherited_property_name in inherited_properties.items():
            inherited_property = (middle_collection, inherited_property_name)
            true_inherited = get_true_property(inherited_property)
            if true_inherited is None:
                if not (
                    primary_property in compound_names
                    and inherited_property_name
                    in compound_inherited_aliases[primary_property]
                ):
                    must_restart = True
                continue

            if true_inherited not in defined:
                new_dependencies.append(true_inherited)
                continue

            add_dependency(property, true_inherited)

        if len(new_dependencies) > 0:
            done = False
            compound_stack.append(property)
            compound_stack.extend(new_dependencies)

        if must_restart:
            done = False
            compound_stack.appendleft(property)
            continue

        if not done:
            continue

        compound_inherited_aliases[property] = compound_inherited_aliases[
            reverse_property
        ] = set(inherited_properties)
        defined.add(property)
        defined.add(reverse_property)

    return dependencies


def topologically_sort_properties(
    raw_properties: List[Tuple[str, str, dict]],
) -> List[Tuple[str, str, dict]]:
    """
    TODO: add function docstring
    """
    reformatted_properties: Dict[Tuple[str, str], Tuple[dict, int]] = {
        property[:2]: (property[2], i) for i, property in enumerate(raw_properties)
    }
    dependencies: List[Set[int]] = get_property_dependencies(reformatted_properties)
    finish_times: List[int] = topological_ordering(dependencies)
    ordered_keys: List[Tuple[str, str]] = sorted(
        reformatted_properties, key=lambda k: finish_times[reformatted_properties[k][1]]
    )
    ordered_properties: List[Tuple[str, str, dict]] = [
        (*k, reformatted_properties[k]) for k in ordered_keys
    ]
    for i, p in enumerate(ordered_properties):
        print(i, p)
    return ordered_properties
