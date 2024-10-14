"""
TODO: add file-level docstring
"""

from typing import Dict, List


class PyDoughMetadataException(Exception):
    """
    TODO: add class docstring
    """


def verify_valid_name(name: str) -> None:
    """
    TODO: add function docstring.
    """
    if not name.isidentifier():
        raise PyDoughMetadataException(
            f"Invalid name {repr(name)}: all PyDough graph/collection/property names must be valid Python identifiers."
        )


def verify_has_type(
    obj: object, typ: type, error_name: str, type_name: str = None
) -> None:
    """
    TODO: add function docstring.
    """
    if not isinstance(obj, typ):
        type_name = typ.__name__ if type_name is None else type_name
        raise PyDoughMetadataException(
            f"{error_name} must be a {type_name}, received: {obj.__class__.__name__}."
        )


def verify_is_list_of_string_or_strings(
    json_obj, error_name: str, allow_empty: bool = False
) -> None:
    """
    TODO: add function docstring.
    """
    if not (
        isinstance(json_obj, list)
        and (
            (allow_empty and len(json_obj) == 0)
            or (
                len(json_obj) > 0
                and all(
                    isinstance(elem, str)
                    or (
                        isinstance(elem, list)
                        and all(isinstance(sub_elem, str) for sub_elem in elem)
                    )
                    for elem in json_obj
                )
            )
        )
    ):
        collection_error_name = "list" if allow_empty else "non-empty list"
        raise PyDoughMetadataException(
            f"{error_name} must be a {collection_error_name} whose elements are all either strings or lists of strings."
        )


def verify_is_json_string_list_mapping(
    json_obj: Dict,
    error_name: str,
    allow_outer_empty: bool = False,
    allow_inner_empty: bool = False,
) -> None:
    verify_has_type(json_obj, dict, error_name)
    if not (
        (allow_outer_empty and len(json_obj) == 0)
        or (
            len(json_obj) > 0
            and all(
                isinstance(elem, list)
                and (
                    (allow_inner_empty and len(elem) == 0)
                    or (
                        len(elem) > 0 and isinstance(sub_elem, str) for sub_elem in elem
                    )
                )
                for elem in json_obj.values()
            )
        )
    ):
        collection_error_name = "mapping" if allow_outer_empty else "non-empty mapping"
        sub_collection_error_name = "lists" if allow_inner_empty else "non-empty lists"
        raise PyDoughMetadataException(
            f"{error_name} must be a {collection_error_name} of strings to {sub_collection_error_name} of strings strings."
        )


def verify_is_json_string_mapping(
    json_obj: Dict, error_name: str, allow_empty: bool = False
) -> None:
    verify_has_type(json_obj, dict, error_name)
    if not (
        (allow_empty and len(json_obj) == 0)
        or (
            len(json_obj) > 0
            and all(isinstance(elem, str) for elem in json_obj.values())
        )
    ):
        collection_error_name = "mapping" if allow_empty else "non-empty mapping"
        raise PyDoughMetadataException(
            f"{error_name} must be a {collection_error_name} of strings to strings strings."
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


def verify_typ_in_json(
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
        property, expected_type, f"Property {repr(property_name)} of {error_name}"
    )


def verify_list_of_string_or_strings_in_json(
    json_obj, property_name: str, error_name: str, allow_empty: bool = False
) -> None:
    """
    TODO: add function docstring.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_is_list_of_string_or_strings(
        property, f"Property {repr(property_name)} of {error_name}", allow_empty
    )


def verify_json_string_list_mapping_in_json(
    json_obj, property_name: str, error_name: str, allow_empty: bool = False
) -> None:
    """
    TODO: add function docstring.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_is_json_string_list_mapping(
        property, f"Property {repr(property_name)} of {error_name}", allow_empty
    )


def verify_json_string_mapping_in_json(
    json_obj, property_name: str, error_name: str, allow_empty: bool = False
) -> None:
    """
    TODO: add function docstring.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_is_json_string_mapping(
        property, f"Property {repr(property_name)} of {error_name}", allow_empty
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


def verify_property_in_object(obj: object, property_name: str, error_name: str) -> None:
    """
    TODO: add function docstring.
    """
    if not hasattr(obj, property_name):
        raise PyDoughMetadataException(
            f"Property {repr(property_name)} of {error_name} is missing."
        )


def verify_typ_in_object(
    obj: object,
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


def verify_json_string_list_mapping_in_object(
    obj: object, property_name: str, error_name: str, allow_empty: bool = False
) -> None:
    """
    TODO: add function docstring.
    """
    verify_property_in_object(obj, property_name, error_name)
    property = getattr(obj, property_name)
    verify_is_json_string_list_mapping(
        property, f"Property {repr(property_name)} of {error_name}", allow_empty
    )


def verify_json_string_mapping_in_object(
    obj: object, property_name: str, error_name: str, allow_empty: bool = False
) -> None:
    """
    TODO: add function docstring.
    """
    verify_property_in_object(obj, property_name, error_name)
    property = getattr(obj, property_name)
    verify_is_json_string_mapping(
        property, f"Property {repr(property_name)} of {error_name}", allow_empty
    )
