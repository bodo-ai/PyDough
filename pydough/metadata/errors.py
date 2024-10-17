"""
TODO: add file-level docstring
"""

from typing import Dict, List, Callable, Any, Iterable


class PyDoughMetadataException(Exception):
    """Exception raised when there is an error relating to PyDough metadata, such
    as an error while parsing/validating the JSON or an ill-formed pattern.
    """


###############################################################################
# Generic validation utilities
###############################################################################


def verify_valid_name(name: str) -> None:
    """
    Verifies that a string can be used as a name for a PyDough graph,
    collection, or property.

    Args:
        `name`: the string being checked.

    Raises:
        `PyDoughMetadataException`: if the name is invalid.
    """
    if not name.isidentifier():
        raise PyDoughMetadataException(
            f"Invalid name {name!r}: all PyDough graph/collection/property names must be valid Python identifiers."
        )


def verify_no_extra_keys_in_json(
    json_obj: dict, allowed_fields: List[str], error_name: str
) -> None:
    """
    Verifies that a JSON object (a dictionary) does not have any extra
    key-value pairs besides the ones listed.

    Args:
        `json_obj`: the dictionary being checked.
        `allowed_fields`: the list of permitted keys.
        `error_name`: a string indicating how `json_obj` should be identified
        if included in an error message.

    Raises:
        `PyDoughMetadataException`: if there is an extra key in the JSON
        object.
    """
    extra_keys = {key for key in json_obj if key not in allowed_fields}
    if len(extra_keys) > 0:
        raise PyDoughMetadataException(
            f"There are unexpected extra properties in {error_name}: {extra_keys}."
        )


def verify_property_in_json(
    json_obj: dict, property_name: str, error_name: str
) -> None:
    """
    Verifies that a JSON object (a dictionary) contains a specific key.

    Args:
        `json_obj`: the dictionary being checked.
        `property_name`: the key being searched for
        `error_name`: a string indicating how `json_obj` should be identified
        if included in an error message.

    Raises:
        `PyDoughMetadataException`: if the property is missing.
    """
    if property_name not in json_obj:
        raise PyDoughMetadataException(
            f"Metadata for {error_name} missing property {property_name!r}."
        )


def verify_has_type(
    obj: Any, expected_type: type, error_name: str, type_name: str | None = None
) -> None:
    """
    Verifies that an object has a specific type.

    Args:
        `obj`: the object being checked.
        `property_name`: the key being searched for
        `expected_type`: the type that the property should be.
        `error_name`: a string indicating how `obj` should be identified
        if included in an error message.
        `type_name`: an optional string indicating how `type` should be
        identified if included in an error message. If omitted, just uses
        the type's name directly.

    Raises:
        `PyDoughMetadataException`: if the types do not match.
    """
    if not isinstance(obj, expected_type):
        type_name = expected_type.__name__ if type_name is None else type_name
        raise PyDoughMetadataException(
            f"{error_name} must be a {type_name}, received: {obj.__class__.__name__}."
        )


def verify_matches_predicate(
    obj: Any, predicate: Callable[[Any], bool], error_name: str, predicate_name: str
) -> None:
    """
    Verifies that an object matches a specific predicate.

    Args:
        `obj`: the object being checked.
        `predicate`: a function that can be called on `obj` that returns
        a boolean indicating success or failure.
        `error_name`: a string indicating how `obj` should be identified
        if included in an error message.
        `predicate_name`: an string indicating what condition the object
        failed to meet if as part of an error message.

    Raises:
        `PyDoughMetadataException`: if the types do not match.
    """
    if not predicate(obj):
        raise PyDoughMetadataException(f"{error_name} must be a {predicate_name}.")


def verify_json_has_property_with_type(
    json_obj: Dict,
    property_name: str,
    expected_type: type,
    error_name: str,
    type_name: str | None = None,
) -> None:
    """
    Verifies that a JSON object (a dictionary) contains a property
    with a certain type.

    Args:
        `json_obj`: the JSON object being checked.
        `property_name`: the key for the property being checked.
        `expected_type`: the type that the property should be.
        `error_name`: a string indicating how `obj` should be identified
        if included in an error message.
        `type_name`: an optional string indicating how `type` should be
        identified if included in an error message. If omitted, just uses
        the type's name directly.

    Raises:
        `PyDoughMetadataException`: if the types do not match.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_has_type(
        property,
        expected_type,
        f"Property {property_name!r} of {error_name}",
        type_name,
    )


def verify_json_has_property_matching(
    json_obj: Dict,
    property_name: str,
    predicate: Callable[[Any], bool],
    error_name: str,
    predicate_str: str,
) -> None:
    """
    Verifies that a JSON object (a dictionary) contains a property
    that passes a certain predicate.

    Args:
        `json_obj`: the JSON object being checked.
        `property_name`: the key for the property being checked.
        `predicate`: a function that can be called on the property that returns
        a boolean indicating success or failure.
        `error_name`: a string indicating how `obj` should be identified
        if included in an error message.
        `predicate_name`: an string indicating what condition the object
        failed to meet if as part of an error message.

    Raises:
        `PyDoughMetadataException`: if the types do not match.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_matches_predicate(
        property,
        predicate,
        f"Property {property_name!r} of {error_name}",
        predicate_str,
    )


###############################################################################
# Specific predicates
###############################################################################


def size_check(obj: Iterable, allow_empty: bool) -> bool:
    """
    Helper utility for other predicates, determining whether an
    iterable passes a size criteria.

    Args:
        `obj`: the iterable being checked.
        `allow_empty`: whether to allow the iterable to be empty.

    Returns:
        True if the iterable is non-empty. If `allow_empty` is True,
        then the check is skipped and always returns True.
    """
    return allow_empty or len(obj) > 0


def is_list_of_strings(obj: Any, allow_empty: bool = False) -> bool:
    """
    Predicate to determine if an object is a list of strings.

    Args:
        `obj`: the object being checked.
        `allow_empty`: whether to allow an empty list.

    Returns:
        True if `obj` meets the criteria.
    """
    return (
        isinstance(obj, list)
        and size_check(obj, allow_empty)
        and all(isinstance(elem, str) for elem in obj)
    )


def is_list_of_strings_or_string_lists(
    obj: Any, outer_allow_empty: bool = False, inner_allow_empty: bool = False
) -> bool:
    """
    Predicate to determine if an object is a list of objects that
    are either strings or lists of strings.

    Args:
        `obj`: the object being checked.
        `outer_allow_empty`: whether to allow the entire object to be an empty
        list.
        `inner_allow_empty`: whether to allow any inner of the inner objects to
        be enpty lists.

    Returns:
        True if `obj` meets the criteria.
    """
    return (
        isinstance(obj, list)
        and size_check(obj, outer_allow_empty)
        and all(
            isinstance(elem, str) or is_list_of_strings(elem, inner_allow_empty)
            for elem in obj
        )
    )


def is_string_string_mapping(obj: Any, allow_empty: bool = False) -> bool:
    """
    Predicate to determine if an object is a mapping of strings to strings.

    Args:
        `obj`: the object being checked.
        `allow_empty`: whether to allow an empty mapping.

    Returns:
        True if `obj` meets the criteria.
    """
    return (
        isinstance(obj, dict)
        and size_check(obj, allow_empty)
        and all(
            isinstance(key, str) and isinstance(val, str) for key, val in obj.items()
        )
    )


def is_string_string_list_mapping(
    obj: Any, outer_allow_empty: bool = False, inner_allow_empty: bool = False
) -> bool:
    """
    Predicate to determine if an object is a mapping of strings to lists
    of strings strings.

    Args:
        `obj`: the object being checked.
        `outer_allow_empty`: whether to allow the entire object to be an empty
        list.
        `inner_allow_empty`: whether to allow any inner of the inner objects to
        be enpty lists.

    Returns:
        True if `obj` meets the criteria.
    """
    return (
        isinstance(obj, dict)
        and size_check(obj, outer_allow_empty)
        and all(
            isinstance(key, str) and is_list_of_strings(val, inner_allow_empty)
            for key, val in obj.items()
        )
    )
