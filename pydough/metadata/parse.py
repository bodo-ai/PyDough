"""
The logic used to parse PyDough metadata from a JSON file.
"""

__all__ = ["parse_json_metadata_from_file"]

import json
from collections import deque
from dataclasses import dataclass

from .collections import CollectionMetadata
from .errors import (
    HasPropertyWith,
    HasType,
    PyDoughMetadataException,
)
from .graphs import GraphMetadata
from .properties import PropertyMetadata


# The way a property is stored until it is parsed.
@dataclass
class RawProperty:
    collection_name: str
    property_name: str
    property_json: dict


# The way a property is referenced as a key in dictionaries or identified
# on stacks.
@dataclass(frozen=True)
class PropertyKey:
    collection_name: str
    property_name: str


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
    with open(file_path) as f:
        as_json = json.load(f)
    if not isinstance(as_json, dict):
        raise PyDoughMetadataException(
            "PyDough metadata expected to be a JSON file containing a JSON "
            + f"object, received: {as_json.__class__.__name__}."
        )
    if graph_name not in as_json:
        raise PyDoughMetadataException(
            f"PyDough metadata file located at {file_path!r} does not "
            + f"contain a graph named {graph_name!r}"
        )
    graph_json = as_json[graph_name]
    return parse_graph(graph_name, graph_json)


def parse_graph(graph_name: str, graph_json: dict) -> GraphMetadata:
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
    HasType(dict).verify(graph_json, "metadata for PyDough graph")
    graph = GraphMetadata(graph_name)

    # A list that will store each collection property in the metadata
    # before it is defined and added to its collection, so all of the properties
    # can be sorted based on their dependencies. The list stores the properties
    # as tuples in the form (collection_name, property_name, property_json)
    raw_properties: list[RawProperty] = []

    # Iterate through all the key-value pairs in the graph to set up the
    # corresponding collections as empty metadata that will later be filled
    # with properties, and also obtain each of the properties.
    for collection_name in graph_json:
        # Add the raw collection metadata to the collections dictionary
        collection_json: dict = graph_json[collection_name]
        CollectionMetadata.parse_from_json(graph, collection_name, collection_json)
        collection = graph.get_collection(collection_name)
        assert isinstance(collection, CollectionMetadata)

        # Add the unprocessed properties of each collection to the properties
        # list (the parsing of the collection verified that the 'properties' key
        # exists). Also, verify that the JSON is well formed.
        properties_json: dict[str, dict] = graph_json[collection_name]["properties"]
        for property_name in properties_json:
            property_json: dict = properties_json[property_name]
            PropertyMetadata.verify_json_metadata(
                collection, property_name, property_json
            )
            raw_property: RawProperty = RawProperty(
                collection_name,
                property_name,
                property_json,
            )
            raw_properties.append(raw_property)

    # Sort the properties and iterate through them in an order such that when
    # a property is reached in the loop, all the properties it depends on are
    # complete. For each property, complete the process of parsing its JSON to
    # add it to its collection's properties.
    ordered_properties = topologically_sort_properties(raw_properties)
    for property in ordered_properties:
        HasPropertyWith(property.collection_name, HasType(CollectionMetadata)).verify(
            graph.collections, graph.error_name
        )
        incomplete_collection = graph.collections[property.collection_name]
        assert isinstance(incomplete_collection, CollectionMetadata)
        PropertyMetadata.parse_from_json(
            incomplete_collection, property.property_name, property.property_json
        )

    # Finally, after every property has been parsed, run an additional round
    # of completeness checks on each collection to verify any predicates about
    # the metadata being well/ill-formatted that are impossible to determine
    # until every property has been defined.
    for collection_name in graph.get_collection_names():
        complete_collection = graph.get_collection(collection_name)
        assert isinstance(complete_collection, CollectionMetadata)
        complete_collection.verify_complete()

    return graph


def topologically_sort_properties(
    raw_properties: list[RawProperty],
) -> list[RawProperty]:
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
    reformatted_properties: dict[PropertyKey, tuple[dict, int]] = {
        PropertyKey(property.collection_name, property.property_name): (
            property.property_json,
            i,
        )
        for i, property in enumerate(raw_properties)
    }

    # Compute the dependencies of each property.
    dependencies: list[set[int]] = get_property_dependencies(reformatted_properties)

    # Use the dependencies to calculate the topological ordering of the
    # properties.
    finish_times: list[int] = topological_ordering(dependencies)
    ordered_keys: list[PropertyKey] = sorted(
        reformatted_properties, key=lambda k: finish_times[reformatted_properties[k][1]]
    )

    # Use the topological ordering to re-construct the same format as the
    # `raw_properties` list, but in the desired order.
    ordered_properties: list[RawProperty] = [
        RawProperty(k.collection_name, k.property_name, reformatted_properties[k][0])
        for k in ordered_keys
    ]
    return ordered_properties


def get_property_dependencies(
    reformatted_properties: dict[PropertyKey, tuple[dict, int]],
) -> list[set[int]]:
    """
    Infers the set of dependencies for each property.

    Args:
        `reformatted_properties`: a dictionary of all the properties in the
        graph as a dictionary with keys in the form
        `(collection_name, property_name)` and values in the form
        `(property_json, idx)` where `idx` is the index that each property
        belongs to in the original list, which should be used by the
        dependencies.

    Returns:
        A list of the set of dependencies for each property, where the
        positions in the list are the indices of the properties and the
        sets contain the indices of the properties they depend on.

    Raises:
        `PyDoughMetadataException`: if the dependencies cannot be inferred,
        e.g. because there is a cyclic dependency or a dependency is not
        defined anywhere in the graph.
    """
    n_properties: int = len(reformatted_properties)
    if n_properties == 0:
        return []
    valid_range = range(n_properties)

    # The list that will store the final dependencies.
    dependencies: list[set[int]] = [set() for _ in valid_range]

    # A set of all properties (as dictionary keys) that have been
    # fully defined by the dependency-searching algorithm.
    defined = set()

    # A dictionary mapping each property to its known reverse
    # property, if one exists.
    reverses: dict[PropertyKey, PropertyKey] = {}

    # A dictionary mapping each property to the name of the collection
    # it maps to, if one exists.
    collections_mapped_to: dict[PropertyKey, str] = {}

    # A dictionary mapping each property to the set of all inherited property
    # names that are associated with it.
    compound_inherited_aliases: dict[PropertyKey, set[str]] = {}

    def get_true_property(property: PropertyKey) -> PropertyKey | None:
        """
        Extracts the true canonical representation of a property.

        Args:
            `property`: the input property in terms of a tuple
            `(collection_name, property_name)`.

        Returns:
            The canonical representative for a property, which could either be
            the input tuple or the tuple for the reverse property. If the
            canonical representation is currently unknown (e.g. because the
            property is the reverse of a property that has not yet been defined)
            returns None.
        """
        if property in reformatted_properties:
            return property
        if property in reverses:
            reverse = reverses[property]
            if reverse in reformatted_properties:
                return reverse
        return None

    def add_dependency(property: PropertyKey, dependency: PropertyKey) -> None:
        """
        Marks a dependency relationship between two properties, implying that
        one of them cannot be defined until after the other has been defined.

        Args:
            `property`: the input property in terms of a tuple
            `(collection_name, property_name)`.
            `dependency`: the property that `property` is dependant on, in the
            same tuple format.

        Raises:
            `PyDoughMetadataException` if the property or the dependency
            is not a valid canonical representative property.
        """
        true_property: PropertyKey | None = get_true_property(property)
        true_dependency: PropertyKey | None = get_true_property(dependency)
        if true_property is None or true_property not in reformatted_properties:
            raise PyDoughMetadataException(
                "Unable to extract dependencies of properties in PyDough "
                + "metadata due to either a dependency not existing or a "
                + "cyclic dependency between properties due to unrecognized "
                + f"property '{property.collection_name}.{property.property_name}'"
            )
        if true_dependency is None or true_dependency not in reformatted_properties:
            raise PyDoughMetadataException(
                "Unable to extract dependencies of properties in PyDough "
                + "metadata due to either a dependency not existing or a "
                + "cyclic dependency between properties due to unrecognized "
                + f"property '{dependency.collection_name}.{dependency.property_name}'"
            )
        property_idx: int = reformatted_properties[true_property][1]
        dependency_idx: int = reformatted_properties[true_dependency][1]
        dependencies[property_idx].add(dependency_idx)

    # The set of all properties that are table columns
    table_columns: set[PropertyKey] = set()

    # The set of all properties that are cartesian products or general joins.
    cartesian_or_general: set[PropertyKey] = set()

    # The set of all properties that are simple joins
    simple_joins: set[PropertyKey] = set()

    # The set of all properties that are compound relationships
    compounds: set[PropertyKey] = set()

    # The "stack" uses to process compound relationship properties. A
    # double-ended queue is used because the algorithm for processing compounds
    # sometimes requires moving a property from the top of the stack to the
    # bottom if it cannot infer what must be defined before the compound can be
    # defined, since that information may not be possible to infer until other
    # items already on the stack have been processed.
    compound_stack: deque[PropertyKey] = deque()

    # Classify every property and add it to the corresponding stack/set.
    for reformatted_property in reformatted_properties:
        property_json, _ = reformatted_properties[reformatted_property]
        match property_json["type"]:
            case "table_column":
                table_columns.add(reformatted_property)
            case "cartesian_product" | "general_join":
                cartesian_or_general.add(reformatted_property)
            case "simple_join":
                simple_joins.add(reformatted_property)
            case "compound":
                compounds.add(reformatted_property)
                compound_stack.append(reformatted_property)
            case typ:
                raise PyDoughMetadataException(
                    f"Unrecognized PyDough collection type: {typ!r}"
                )

    # Mark every table column as defined.
    for table_property in table_columns:
        defined.add(table_property)

    def define_cartesian_or_general_property(property: PropertyKey) -> None:
        """
        Defines a cartesian product or general join property, adding its
        dependencies to the datastructure and marking the property as defined
        so subsequent properties can use it as a dependency.

        Args:
            `property`: the property that the algorithm is attempting to
            define, in terms of a tuple `(collection_name, property_name)`.

        Raises:
            `PyDoughMetadataError`: if the properties or relationships are
            malformed.
        """
        property_json, _ = reformatted_properties[property]
        reverse_collection: str = property_json["other_collection_name"]
        reverse_property: str = property_json["reverse_relationship_name"]
        reverse: PropertyKey = PropertyKey(reverse_collection, reverse_property)
        reverses[property] = reverse
        reverses[reverse] = property
        collections_mapped_to[property] = reverse_collection
        collections_mapped_to[reverse] = property.collection_name
        defined.add(property)
        defined.add(reverse)

    # Define every cartesian property
    for cart_or_general_property in cartesian_or_general:
        define_cartesian_or_general_property(cart_or_general_property)

    def define_simple_join_property(property: PropertyKey) -> None:
        """
        Defines a simple join property, adding its dependencies
        to the datastructure and marking the property as defined so
        subsequent properties can use it as a dependency.

        Args:
            `property`: the property that the algorithm is attempting to
            define, in terms of a tuple `(collection_name, property_name)`.

        Raises:
            `PyDoughMetadataError`: if the properties or relationships are
            malformed.
        """
        # The simple join definition process is a superset of the same process
        # for cartesian products.
        define_cartesian_or_general_property(property)
        property_json, _ = reformatted_properties[property]
        collection: str = property.collection_name
        other_collection: str = property_json["other_collection_name"]
        keys: dict[str, list[str]] = property_json["keys"]
        for key_property_name in keys:
            key_property: PropertyKey = PropertyKey(collection, key_property_name)
            add_dependency(property, key_property)
            for match_property_name in keys[key_property_name]:
                match_property: PropertyKey = PropertyKey(
                    other_collection,
                    match_property_name,
                )
                add_dependency(property, match_property)

    # Define every simple join property
    for join_property in simple_joins:
        define_simple_join_property(join_property)

    def attempt_to_defined_compound_relationship(property: PropertyKey) -> bool:
        """
        Procedure that attempts to process a compound property and infer its
        dependencies. If this is not possible because its dependencies are
        still unknown (e.g. they are the reverse of a property that has not
        been defined yet), places the property at the bottom of the stack.
        If a dependency is known but not yet defined, pushes the property back
        on top of the stack underneath its dependency.

        Args:
            `property`: the property that the algorithm is attempting to
            define, in terms of a tuple `(collection_name, property_name)`.

        Returns:
            Whether the attempt succeeded or not. Success is defined as
            property being added to `defined`, rather than pushed back
            onto the stack (either at the top or bottom).

        Raises:
            `PyDoughMetadataError`: if the properties or relationships are
            malformed.
        """
        if property in defined:
            return False

        property_json, _ = reformatted_properties[property]

        primary_property_name: str = property_json["primary_property"]
        primary_property: PropertyKey = PropertyKey(
            property.collection_name,
            primary_property_name,
        )

        # If the primary is defined (including as a reverse), identify the
        # middle collection via collections_mapped_to. If the primary is not
        # defined but is known, push to the stack & move on. If the primary is
        # not defined but is unknown (possibly a reverse), move the compound
        # back to the bottom of the stack.
        true_primary: PropertyKey | None = get_true_property(primary_property)
        if true_primary is None:
            compound_stack.appendleft(property)
            return False

        if true_primary not in defined:
            compound_stack.append(property)
            compound_stack.append(true_primary)
            return False

        middle_collection: str = collections_mapped_to[primary_property]
        secondary_property_name: str = property_json["secondary_property"]
        secondary_property: PropertyKey = PropertyKey(
            middle_collection,
            secondary_property_name,
        )

        # If the secondary is defined (including as a reverse), identify the
        # middle collection via collections_mapped_to. If the secondary is not
        # defined but is known, push to the stack & move on. If the secondary
        # is not defined but is unknown (possibly a reverse), move the compound
        # back to the bottom of the stack.
        true_secondary: PropertyKey | None = get_true_property(secondary_property)
        if true_secondary is None:
            compound_stack.appendleft(property)
            return False

        if true_secondary not in defined:
            compound_stack.append(property)
            compound_stack.append(true_secondary)
            return False

        # Now that the secondary property is known, identify the middle
        # collection, construct the reverse property of the compound,
        # mark the property and its reverse as reverses of one another,
        # and mark both collection's other-collection.
        target_collection: str = collections_mapped_to[secondary_property]
        reverse_property_name: str = property_json["reverse_relationship_name"]
        reverse_property: PropertyKey = PropertyKey(
            target_collection, reverse_property_name
        )
        collections_mapped_to[property] = target_collection
        collections_mapped_to[reverse_property] = property.collection_name
        reverses[property] = reverse_property
        reverses[reverse_property] = property
        compounds.add(reverse_property)

        # Iterate across the inherited properties of the compound relationship
        # to identify which ones are defined, known but undefined, or unknown.
        # When identifying an inherited property, can check if it is a known
        # property of the middle collection or if it is a known inherited
        # property alias of the primary/secondary property.
        inherited_properties: dict[str, str] = property_json["inherited_properties"]
        inherited_dependencies: list[PropertyKey] = []
        undefined_inherited_dependencies: list[PropertyKey] = []
        has_unknown_inherited: bool = False
        for inherited_property_name in inherited_properties.values():
            inherited_property: PropertyKey = PropertyKey(
                middle_collection, inherited_property_name
            )
            true_inherited: PropertyKey | None = get_true_property(inherited_property)
            if true_inherited is None:
                # If the inherited property is unknown because it is actually
                # an inherited property of the primary or secondary property,
                # ignore it since it must have already been defined for the
                # primary or secondary property to be defined, and it does not
                # need to be a dependency of the current property since it is
                # a dependency of the primary or secondary (which this property
                # will be dependent on).
                if not (
                    primary_property in compounds
                    and inherited_property_name
                    in compound_inherited_aliases[true_primary]
                ) and not (
                    secondary_property in compounds
                    and inherited_property_name
                    in compound_inherited_aliases[true_secondary]
                ):
                    has_unknown_inherited = True
                continue

            if true_inherited in defined:
                inherited_dependencies.append(true_inherited)
            else:
                undefined_inherited_dependencies.append(true_inherited)

        # If any of the inherited properties were known but undefined,
        # define those first before resuming the attempt to define this
        # property.
        if len(undefined_inherited_dependencies):
            compound_stack.append(property)
            compound_stack.extend(undefined_inherited_dependencies)
            return False

        # If any of the inherited properties were unknown, place the compound
        # at the bottom of the stack so it can be re-examined after everything
        # else on the stack has been examined, likely meaning that the unknown
        # dependency is now known.
        if has_unknown_inherited:
            compound_stack.appendleft(property)
            return False

        # Declare the primary property, secondary property, and the sources of
        # the inherited properties as a dependencies of the compound. Then,
        # mark the compound as done by adding it and its reverse to the
        # defined set, adding the compound inherited aliases associated with
        # the newly defined property so that compounds depending on this
        # compound can use its inherited properties as inherited properties.
        add_dependency(property, true_primary)
        add_dependency(property, true_secondary)
        for inherited_property in inherited_dependencies:
            add_dependency(property, inherited_property)
        compound_inherited_aliases[property] = compound_inherited_aliases[
            reverse_property
        ] = set(inherited_properties)
        defined.add(property)
        defined.add(reverse_property)
        return True

    # The number of calls to `attempt_to_defined_compound_relationship` since
    # the last successful attempt (resulted in a compound being defined). Used
    # to catch cases where a property is infinitely being popped from the top
    # of the stack then moved to the bottom because its dependencies are truly
    # undefined or are cyclic.
    iters_since_change: int = 0

    # Repeatedly iterate until the 'stack' of compounds is empty. Uses
    # `max_iters_since_change` as a heuristic for when to cut off the
    # algorithm in case it is going on forever due to a cyclic dependency
    # causing the same properties to be repeatedly appended to the stack
    # without being popped.
    max_iters_since_change: int = 2 * len(compound_stack)
    while len(compound_stack) > 0:
        if (
            iters_since_change > len(compound_stack)
            or iters_since_change > max_iters_since_change
        ):
            raise PyDoughMetadataException(
                "Unable to extract dependencies of properties in PyDough "
                + "metadata due to either a dependency not existing or a "
                + "cyclic dependency between properties"
            )
        property: PropertyKey = compound_stack.pop()
        successfully_defined: bool = attempt_to_defined_compound_relationship(property)
        # Update `iters_since_change` based on whether the most recent
        # attempt succeeded in defining `property` or not.
        if successfully_defined:
            iters_since_change = 0
        else:
            iters_since_change += 1

    # Post-process the dependencies by having each property without any
    # dependencies depend on the previous such property, thus ensuring that
    # the topological ordering is "stable" with regards to the order that
    # column properties were defined in the metadata.
    prev_no_dependency_idx: int = -1
    for idx in range(len(dependencies)):
        dependency_set: set[int] = dependencies[idx]
        if len(dependencies[idx]) == 0:
            if prev_no_dependency_idx >= 0:
                dependency_set.add(prev_no_dependency_idx)
            prev_no_dependency_idx = idx

    return dependencies


def topological_ordering(
    dependencies: list[set[int]],
) -> list[int]:
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

    For example, suppose the dependencies list is as follows:
    `[set(), {0}, {3}, {4} {0, 1}]`. This means that the result list `R` must
    be a permutation `range(5)` with the following properties:
    - `R[1] > R[0]`
    - `R[2] > R[3]`
    - `R[3] > R[4]`
    - `R[4] > max(R[0], R[1])`

    Therefore, the following would be a valid answer: `[0, 1, 3, 4, 2]`
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
                    "Cyclic dependency detected between properties in "
                    + "PyDough metadata graph."
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
    malformed_msg: str = "Malformed topological sorting output"
    if len(finish_times) != n_vertices or set(finish_times) != set(valid_range):
        raise PyDoughMetadataException(malformed_msg)
    for idx in valid_range:
        for dependency in dependencies[idx]:
            if finish_times[idx] <= finish_times[dependency]:
                raise PyDoughMetadataException(malformed_msg)

    return finish_times
