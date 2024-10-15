"""
TODO: add file-level docstring
"""

from typing import Dict, List, Callable, Any, Iterable


class PyDoughMetadataException(Exception):
    """
    TODO: add class docstring
    """


###############################################################################
# Generic validation utilities
###############################################################################


def verify_valid_name(name: str) -> None:
    """
    TODO: add function docstring.
    """
    if not name.isidentifier():
        raise PyDoughMetadataException(
            f"Invalid name {repr(name)}: all PyDough graph/collection/property names must be valid Python identifiers."
        )


def verify_no_extra_keys_in_json(
    json_obj: Dict, allowed_properties: List[str], error_name: str
) -> None:
    """
    TODO: add function docstring.
    """
    extra_keys = {key for key in json_obj if key not in allowed_properties}
    if len(extra_keys) > 0:
        raise PyDoughMetadataException(
            f"There are unexpected extra properties in {error_name}: {extra_keys}."
        )


def verify_property_in_json(
    json_obj: Dict, property_name: str, error_name: str
) -> None:
    """
    TODO: add function docstring.
    """
    if property_name not in json_obj:
        raise PyDoughMetadataException(
            f"Metadata for {error_name} missing property {repr(property_name)}."
        )


def verify_property_in_object(obj: object, property_name: str, error_name: str) -> None:
    """
    TODO: add function docstring.
    """
    if not hasattr(obj, property_name):
        raise PyDoughMetadataException(
            f"Property {repr(property_name)} of {error_name} is missing."
        )


def verify_has_type(obj: Any, typ: type, error_name: str, type_name: str) -> None:
    """
    TODO: add function docstring.
    """
    if not isinstance(obj, typ):
        type_name = typ.__name__ if type_name is None else type_name
        raise PyDoughMetadataException(
            f"{error_name} must be a {type_name}, received: {obj.__class__.__name__}."
        )


def verify_matches_predicate(
    obj: Any, predicate: Callable[[Any], bool], error_name: str, predicate_name: str
) -> None:
    """
    TODO: add function docstring.
    """
    if not predicate(obj):
        raise PyDoughMetadataException(f"{error_name} must be a {predicate_name}.")


def verify_json_has_property_with_type(
    json_obj: Dict,
    property_name: str,
    expected_type: type,
    error_name: str,
    type_name: str = None,
) -> None:
    """
    TODO: add function docstring.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_has_type(
        property,
        expected_type,
        f"Property {repr(property_name)} of {error_name}",
        type_name,
    )


def verify_object_has_property_with_type(
    obj: Any,
    property_name: str,
    expected_type: type,
    error_name: str,
    type_name: str = None,
) -> None:
    """
    TODO: add function docstring.
    """
    verify_property_in_object(obj, property_name, error_name)
    property = getattr(obj, property_name)
    verify_has_type(
        property,
        expected_type,
        f"Property {repr(property_name)} of {error_name}",
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
    TODO: add function docstring.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_matches_predicate(
        property,
        predicate,
        f"Property {repr(property_name)} of {error_name}",
        predicate_str,
    )


def verify_object_has_property_matching(
    obj: Any,
    property_name: str,
    predicate: Callable[[Any], bool],
    error_name: str,
    predicate_str: str,
) -> None:
    """
    TODO: add function docstring.
    """
    verify_property_in_object(obj, property_name, error_name)
    property = getattr(obj, property_name)
    verify_matches_predicate(
        property,
        predicate,
        f"Property {repr(property_name)} of {error_name}",
        predicate_str,
    )


###############################################################################
# Specific predicates
###############################################################################


def size_check(obj: Iterable, allow_empty: bool) -> bool:
    """
    TODO: add function docstring.
    """
    return allow_empty or len(obj) > 0


def is_list_of_strings(obj: Any, allow_empty: bool = False) -> bool:
    """
    TODO: add function docstring.
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
    TODO: add function docstring.
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
    TODO: add function docstring.
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
    TODO: add function docstring.
    """
    return (
        isinstance(obj, dict)
        and size_check(obj, outer_allow_empty)
        and all(
            isinstance(key, str) and is_list_of_strings(val, inner_allow_empty)
            for key, val in obj.items()
        )
    )
