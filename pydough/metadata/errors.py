from typing import Dict, List


class PyDoughMetadataException(Exception):
    """
    TODO: add class docstring
    """


def verify_is_json(json_obj, error_name: str) -> None:
    """
    TODO: add function doscstring.
    """
    if not isinstance(json_obj, dict):
        raise PyDoughMetadataException(f"{error_name} must be a JSON object.")


def verify_is_string(json_obj, error_name: str) -> None:
    """
    TODO: add function doscstring.
    """
    if not isinstance(json_obj, str):
        raise PyDoughMetadataException(f"{error_name} must be a string.")


def verify_is_list_of_string_or_strings(
    json_obj, error_name: str, allow_empty: bool = False
) -> None:
    """
    TODO: add function doscstring.
    """
    if not (
        isinstance(property, list)
        and (
            (allow_empty and len(property) == 0)
            or (
                len(property) > 0
                and all(
                    isinstance(elem, str)
                    or (
                        isinstance(elem, list)
                        and all(isinstance(sub_elem, str) for sub_elem in elem)
                    )
                    for elem in property
                )
            )
        )
    ):
        collection_error_name = "list" if allow_empty else "non-empty list"
        raise PyDoughMetadataException(
            f"{error_name} must be a {collection_error_name} whose elements are all either strings or lists of strings."
        )


def verify_property_in_json(
    json_obj: Dict, property_name: str, error_name: str
) -> None:
    """
    TODO: add function doscstring.
    """
    if property_name not in json_obj:
        raise PyDoughMetadataException(
            f"Metadata for {error_name} missing required property {repr(property_name)}."
        )


def verify_string_in_json(json_obj: Dict, property_name: str, error_name: str) -> None:
    """
    TODO: add function doscstring.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_is_string(property, f"Property {repr(property_name)} of {error_name}")


def verify_json_in_json(json_obj: Dict, property_name: str, error_name: str) -> None:
    """
    TODO: add function doscstring.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_is_json(property, f"Property {repr(property_name)} of {error_name}")


def verify_list_of_string_or_strings_in_json(
    json_obj, property_name: str, error_name: str, allow_empty: bool = False
) -> None:
    """
    TODO: add function doscstring.
    """
    verify_property_in_json(json_obj, property_name, error_name)
    property = json_obj[property_name]
    verify_is_list_of_string_or_strings(
        property, f"Property {repr(property_name)} of {error_name}"
    )


def verify_no_extra_keys_in_json(
    json_obj: Dict, allowed_properties: List[str], error_name: str
) -> None:
    """
    TODO: add function doscstring.
    """
    extra_keys = {key for key in json_obj if key not in allowed_properties}
    if len(extra_keys) > 0:
        raise PyDoughMetadataException(
            f"There are unexpected extra properties in {error_name}: {extra_keys}"
        )
