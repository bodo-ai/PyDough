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


def parse_json_metadata_from_file(file_path: str, graph_name: str) -> GraphMetadata:
    """
    Reads a JSON file to obtain a specific PyDough metadata graph.

    Args:
        `file_path`: the path to the file containing the PyDough metadata for
        the desired graph. This should be a JSON file.
        `graph_name`: the name of the graph from the metadata file that is
        being requested. This should be a key in the JSON file.

    Returns:
        The metadata for the PyDough graph, including all of the collections
        and properties defined within.

    Raises:
        `PyDoughMetadataException`: if the file is malformed in any way that
        prevents parsing it to obtain the desired graph.
    """
    with open(file_path, "r") as f:
        as_json = json.load(f)
    if not isinstance(as_json, dict):
        raise PyDoughMetadataException(
            f"PyDough metadata expected to be a JSON file containing a JSON object, received: {as_json.__class__.__name__}."
        )
    if graph_name not in as_json:
        raise PyDoughMetadataException(
            f"PyDough metadata file located at {file_path!r} does not contain a graph named {graph_name!r}"
        )
    graph_json = as_json[graph_name]
    return parse_graph(graph_name, graph_json)


def parse_graph(graph_name: str, graph_json: Dict) -> GraphMetadata:
    """
    Parses a JSON object to obtain the metadata for a PyDough graph.

    Args:
        `graph_name`: the name of the graph being parsed.
        `graph_json`: the JSON object representing the contents
        of the graph.

    Returns:
        The metadata for the PyDough graph, including all of the collections
        and properties defined within.

    Raises:
        `PyDoughMetadataException`: if the JSON is malformed in any way that
        prevents parsing it to obtain the desired graph.
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
        collection_json: dict = graph_json[collection_name]
        CollectionMetadata.parse_from_json(graph, collection_name, collection_json)
        collection: CollectionMetadata = graph.get_collection(collection_name)

        # Add the unprocessed properties of each collection to the properties
        # list (the parsing of the collection verified that the 'properties' key
        # exists). Also, verify that the JSON is well formed.
        properties_json: Dict[str, dict] = graph_json[collection_name]["properties"]
        for property_name in properties_json:
            property_json: dict = properties_json[property_name]
            PropertyMetadata.verify_json_metadata(
                collection, property_name, property_json
            )
            raw_properties.append((collection_name, property_name, property_json))

    ordered_properties = topologically_sort_properties(raw_properties)
    for collection_name, property_name, property_json in ordered_properties:
        verify_json_has_property_with_type(
            graph.collections, collection_name, CollectionMetadata, graph.error_name
        )
        collection = graph.collections[collection_name]
        PropertyMetadata.parse_from_json(collection, property_name, property_json)

    return graph


def topological_ordering(dependencies: List[Set[int]]) -> List[int]:
    """
    Computes a topological ordering of a list of objects with dependencies,
    assuming that the dependencies correspond to a directed acyclic graph.

    Args:
        `dependencies`: a list mapping each object by its position to the
        indices of all objects that it depends on.

    Returns:
        The topological ordering of the objects as a list of integers where the
        value at each index corresponds to the order with which the
        corresponding item should be visited in order to ensure it is visited
        after all of its dependencies.

    Raises:
        `PyDoughMetadataException`: if the inputs are malformed, e.g. because
        they contain invalid dependency indices or there is a cycle.
    """
    n_vertices = len(dependencies)
    valid_range = range(n_vertices)

    # The list containing the final output, where `finish_times[i]` is ordinal
    # position that index `i` should be visited at in order to guarantee that
    # it is visited after all of its dependencies.
    finish_times = [-1 for _ in valid_range]

    # A set of all indices that have already been visited.
    visited = set()

    # A counter keeping track of the number of indices that have already had
    # their finish times computed, meaning it is safe to compute the finish
    # time of vertices that depends on them.
    current_time = 0

    # A set of all indices that are in the ancestry tree of the recursive
    # backtracking's current step. If a neighbor is encountered that is
    # in this set, it means there is a cycle in the dependencies.
    ancestry = set()

    # Recursive backtracking function that traverses the dependencies
    # starting from `idx`.
    def dfs(idx: int):
        nonlocal current_time

        # Do not visit an index after it has already been visited.
        if idx in visited:
            return

        # Mark this index as visited and also add it to the ancestry so that if
        # any recursive descendants of this call reach this index again, the
        # cycle will be detected.
        visited.add(idx)
        ancestry.add(idx)

        # Iterate across all dependencies of the vertex, making sure they are well
        # formed and do not indicate a cycle, then recursively visit them.
        for dependency in dependencies[idx]:
            if dependency not in valid_range:
                raise PyDoughMetadataException(
                    "Malformed property dependencies detected."
                )
            if dependency in ancestry:
                raise PyDoughMetadataException(
                    "Cyclic dependency detected between properties in PyDough metadata graph."
                )
            if dependency not in visited:
                dfs(dependency)

        # Once all dependencies of the index have been visited, set the finish
        # time of the current index then increment the timer so subsequently
        # finished indices are known to come after this index.
        finish_times[idx] = current_time
        current_time += 1

        # Remove the index from the ancestry set so later recursive calls do
        # not confuse multiple indices being dependant on the same index with
        # having an actual cycle
        ancestry.discard(idx)

    # Iterate across all indices and invoke the recursive procedure on each
    # of them, since the indices could be a disconnected forest of DAGs.
    for idx in valid_range:
        dfs(idx)

    # Verify that the final output list is well-formed, meaning that it is a
    # list of the correct length containing the desired integers where each
    # index has a finish time that is larger than all of its dependencies.
    if len(finish_times) != n_vertices or set(finish_times) != set(valid_range):
        raise PyDoughMetadataException("Malformed topological sorting output")
    for idx in valid_range:
        for dependency in dependencies[idx]:
            if finish_times[idx] <= finish_times[dependency]:
                raise PyDoughMetadataException("Malformed topological sorting output")

    return finish_times


def get_property_dependencies(
    reformatted_properties: Dict[Tuple[str, str], Tuple[dict, int]],
) -> List[Set[int]]:
    """
    TODO: add function docstring

    !!! THIS FUNCTION IS A WORK IN PROGRESS !!!
    """
    n_properties = len(reformatted_properties)
    if n_properties == 0:
        return []

    valid_range = range(n_properties)
    dependencies: List[Set[int]] = [set() for i in valid_range]
    defined = set()

    compound_stack: deque = deque()
    reverses: Dict[Tuple[str, str], Tuple[str, str]] = {}
    collections_mapped_to: Dict[Tuple[str, str], str] = {}
    compound_inherited_aliases: Dict[Tuple[str, str], Set[str]] = {}

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
        if true_property is None or true_property not in reformatted_properties:
            raise PyDoughMetadataException(
                f"Unable to extract dependencies of properties in PyDough metadata due to unrecognized property {property}"
            )
        if true_dependency is None or true_dependency not in reformatted_properties:
            raise PyDoughMetadataException(
                f"Unable to extract dependencies of properties in PyDough metadata due to unrecognized property {dependency}"
            )
        property_idx = reformatted_properties[true_property][1]
        dependency_idx = reformatted_properties[true_dependency][1]
        dependencies[property_idx].add(dependency_idx)

    table_columns: Set[Tuple[str, str]] = set()
    cartesian_products: Set[Tuple[str, str]] = set()
    simple_joins: Set[Tuple[str, str]] = set()
    compounds: Set[Tuple[str, str]] = set()
    for property in reformatted_properties:
        property_json, _ = reformatted_properties[property]
        match property_json["type"]:
            case "table_column":
                table_columns.add(property)
            case "cartesian_product":
                cartesian_products.add(property)
            case "simple_join":
                simple_joins.add(property)
            case "compound":
                compounds.add(property)
                compound_stack.append(property)
            case typ:
                raise PyDoughMetadataException(
                    f"Unrecognized PyDough collection type: {typ!r}"
                )

    for property in table_columns:
        defined.add(property)

    def define_cartesian_property(property: Tuple[str, str]) -> None:
        property_json, _ = reformatted_properties[property]
        reverse_collection = property_json["other_collection_name"]
        reverse_property = property_json["reverse_relationship_name"]
        reverse = (reverse_collection, reverse_property)
        reverses[property] = reverse
        reverses[reverse] = property
        collections_mapped_to[property] = reverse_collection
        collections_mapped_to[reverse] = property[0]
        defined.add(property)
        defined.add(reverse)

    def define_simple_join_property(property: Tuple[str, str]) -> None:
        define_cartesian_property(property)
        property_json, _ = reformatted_properties[property]
        collection = property[0]
        other_collection = property_json["other_collection_name"]
        keys = property_json["keys"]
        for key_property_name in keys:
            key_property = (collection, key_property_name)
            add_dependency(property, key_property)
            for match_property_name in keys[key_property_name]:
                match_property = (other_collection, match_property_name)
                add_dependency(property, match_property)

    for property in cartesian_products:
        define_cartesian_property(property)

    for property in simple_joins:
        define_simple_join_property(property)

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

    iters_since_change: int = 0
    max_iters_since_change: int = 2 * len(compound_stack)
    while len(compound_stack) > 0:
        if (
            iters_since_change > len(compound_stack)
            or iters_since_change > max_iters_since_change
        ):
            raise PyDoughMetadataException(
                "Unable to extract dependencies of properties in PyDough metadata"
            )
        property: Tuple[str, str] = compound_stack.pop()
        if property in defined:
            continue
        iters_since_change += 1
        property_json, _ = reformatted_properties[property]

        primary_property_name: str = property_json["primary_property"]
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
        secondary_property_name: str = property_json["secondary_property"]
        secondary_property: Tuple[str, str] = (
            middle_collection,
            secondary_property_name,
        )

        true_secondary = get_true_property(secondary_property)
        if true_secondary is None:
            compound_stack.appendleft(property)
            continue

        if true_secondary not in defined:
            compound_stack.append(property)
            compound_stack.append(true_secondary)
            continue

        add_dependency(property, true_secondary)

        target_collection: str = collections_mapped_to[secondary_property]
        reverse_property_name = property_json["reverse_relationship_name"]
        reverse_property = (target_collection, reverse_property_name)
        collections_mapped_to[property] = target_collection
        collections_mapped_to[reverse_property] = original_collection
        reverses[property] = reverse_property
        reverses[reverse_property] = property
        compounds.add(reverse_property)

        inherited_properties: Dict[str, str] = property_json["inherited_properties"]
        new_dependencies = []
        must_restart = False
        done = True
        for alias, inherited_property_name in inherited_properties.items():
            inherited_property = (middle_collection, inherited_property_name)
            true_inherited = get_true_property(inherited_property)
            if true_inherited is None:
                if not (
                    primary_property in compounds
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
        iters_since_change = 0

    return dependencies


def topologically_sort_properties(
    raw_properties: List[Tuple[str, str, dict]],
) -> List[Tuple[str, str, dict]]:
    """
    Computes the ordered that each property should be defined in so that
    all dependencies of the property have been defined first.

    Args:
        `raw_properties`: a list of tuples representing each property in
        the form `(collection_name, property_name, property_json)`.

    Returns:
        A list identical to `raw_properties` except that it has been reordered
        so that each property is defined after all properties it depends on.

    Raises:
        `PyDoughMetadataException`: if the inputs are malformed, e.g. because
        the JSON of the properties refers to missing collections/properties,
        or if there is a cycle in the dependencies of properties.
    """
    # Reformat the properties list into a dictionary where the keys are the
    # identifying `(collection_name, property_name)` tuple (hereafter
    # referred to as the `property`) and the values are a tuple of the
    # property's JSON and its index in the original raw_properties list.
    reformatted_properties: Dict[Tuple[str, str], Tuple[dict, int]] = {
        property[:2]: (property[2], i) for i, property in enumerate(raw_properties)
    }

    # Compute the dependencies of each property.
    dependencies: List[Set[int]] = get_property_dependencies(reformatted_properties)

    # Use the dependencies to calculate the topological ordering of the
    # properties.
    finish_times: List[int] = topological_ordering(dependencies)
    ordered_keys: List[Tuple[str, str]] = sorted(
        reformatted_properties, key=lambda k: finish_times[reformatted_properties[k][1]]
    )

    # Use the topological ordering to re-construct the same format as the
    # `raw_properties`` list, but in the desired order.
    ordered_properties: List[Tuple[str, str, dict]] = [
        k + (reformatted_properties[k][0],) for k in ordered_keys
    ]
    return ordered_properties
