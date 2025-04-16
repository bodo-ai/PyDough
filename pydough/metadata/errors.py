"""
The definitions of error-handling utilities for the PyDough metadata module.
"""

__all__ = [
    "ContainsField",
    "HasType",
    "ListOf",
    "MapOf",
    "NoExtraKeys",
    "NonEmptyListOf",
    "NonEmptyMapOf",
    "OrCondition",
    "PossiblyEmptyListOf",
    "PossiblyEmptyMapOf",
    "PyDoughMetadataException",
    "PyDoughPredicate",
    "compound_relationship_inherited_predicate",
    "is_bool",
    "is_integer",
    "is_positive_int",
    "is_string",
    "is_valid_name",
    "simple_join_keys_predicate",
    "unique_properties_predicate",
]


from abc import ABC, abstractmethod


class PyDoughMetadataException(Exception):
    """Exception raised when there is an error relating to PyDough metadata, such
    as an error while parsing/validating the JSON or an ill-formed pattern.
    """


###############################################################################
# Predicate Classes
###############################################################################


class PyDoughPredicate(ABC):
    """Abstract base class for predicates that can be used to verify that
    objects in the PyDough metadata meet certain properties. Each
    implementation must implement the following:
    - `accept`
    - `error_message`
    """

    @abstractmethod
    def accept(self, obj: object) -> bool:
        """
        Takes in an object and returns true if it satisfies the predicate.

        Arguments:
            `obj`: the object to check.

        Returns:
            A boolean value indicating if `obj` satisfied the predicate.
        """

    @abstractmethod
    def error_message(self, error_name: str) -> str:
        """
        Produces the error message to indicate that the predicate failed.

        Arguments:
            `error_name`: the name to refer to the object that failed to
            meet the predicate.

        Returns:
            A string to be used in error messages.
        """

    def verify(self, obj: object, error_name: str) -> None:
        """
        Takes in an object and verifies true if it satisfies the predicate,
        raising an exception otherwise.

        Arguments:
            `obj`: the object to check.
            `error_name`: the name to refer to `obj` by in error messages.

        Raises:
            `PyDoughMetadataException`: if `obj` did not satisfy the predicate.
        """
        if not self.accept(obj):
            raise PyDoughMetadataException(self.error_message(error_name))


class ValidName(PyDoughPredicate):
    """Predicate class to check that an object is a string that can be used
    as the name of a PyDough graph/collection/property.
    """

    def accept(self, obj: object) -> bool:
        return isinstance(obj, str) and obj.isidentifier()

    def error_message(self, error_name: str) -> str:
        return f"{error_name} must be a string that is a Python identifier"


class NoExtraKeys(PyDoughPredicate):
    """Predicate class to check that a JSON object does not have extra fields
    besides those that have been specified.
    """

    def __init__(self, valid_keys: set[str]):
        self.valid_keys: set[str] = valid_keys

    def accept(self, obj: object) -> bool:
        return isinstance(obj, dict) and set(obj) <= self.valid_keys

    def error_message(self, error_name: str) -> str:
        return f"{error_name} must be a JSON object containing no fields except for {sorted(self.valid_keys)!r}"


class ContainsField(PyDoughPredicate):
    """Predicate class to check that a JSON object contains a field
    with a certain name.
    """

    def __init__(self, field_name: str):
        self.field_name: str = field_name

    def accept(self, obj: object) -> bool:
        return isinstance(obj, dict) and self.field_name in obj

    def error_message(self, error_name: str) -> str:
        return (
            f"{error_name} must be a JSON object containing a field {self.field_name!r}"
        )


class HasType(PyDoughPredicate):
    """Predicate class to check that an object has a certain type"""

    def __init__(self, desired_type: type, type_name: str | None = None):
        self.desired_type: type = desired_type
        self.type_name: str = (
            self.desired_type.__name__ if type_name is None else type_name
        )

    def accept(self, obj: object) -> bool:
        return isinstance(obj, self.desired_type)

    def error_message(self, error_name: str) -> str:
        return f"{error_name} must be a {self.type_name}"


class HasPropertyWith(PyDoughPredicate):
    """Predicate class to check that an object has a field matching a predicate"""

    def __init__(self, field_name: str, field_predicate: PyDoughPredicate):
        self.field_name = field_name
        self.has_predicate: PyDoughPredicate = ContainsField(field_name)
        self.field_predicate: PyDoughPredicate = field_predicate

    def accept(self, obj: object) -> bool:
        if not self.has_predicate.accept(obj):
            return False
        assert isinstance(obj, dict)
        return self.field_predicate.accept(obj[self.field_name])

    def error_message(self, error_name: str) -> str:
        lhs = self.has_predicate.error_message(error_name)
        rhs = self.field_predicate.error_message(f"field {self.field_name!r}")
        return f"{lhs} and {rhs}"


class ListOf(PyDoughPredicate):
    """Predicate class to check that an object is a list whose elements
    match another predicate.
    """

    def __init__(self, element_predicate: PyDoughPredicate, allow_empty: bool):
        self.element_predicate: PyDoughPredicate = element_predicate
        self.allow_empty: bool = allow_empty

    def accept(self, obj: object) -> bool:
        return (
            isinstance(obj, list)
            and (self.allow_empty or len(obj) > 0)
            and all(self.element_predicate.accept(elem) for elem in obj)
        )

    def error_message(self, error_name: str) -> str:
        elem_msg = self.element_predicate.error_message("each element")
        collection_name = "list" if self.allow_empty else "non-empty list"
        return f"{error_name} must be a {collection_name} where {elem_msg}"


class PossiblyEmptyListOf(ListOf):
    """Predicate class to check that an object is a list whose elements
    match another predicate, allowing empty lists.
    """

    def __init__(self, element_predicate: PyDoughPredicate):
        super().__init__(element_predicate, True)


class NonEmptyListOf(ListOf):
    """Predicate class to check that an object is a list whose elements
    match another predicate, not allowing empty lists.
    """

    def __init__(self, element_predicate: PyDoughPredicate):
        super().__init__(element_predicate, False)


class MapOf(PyDoughPredicate):
    """Predicate class to check that a dictionary with certain predicates for
    its keys and values.
    """

    def __init__(
        self,
        key_predicate: PyDoughPredicate,
        val_predicate: PyDoughPredicate,
        allow_empty: bool,
    ):
        self.key_predicate: PyDoughPredicate = key_predicate
        self.val_predicate: PyDoughPredicate = val_predicate
        self.allow_empty: bool = allow_empty

    def accept(self, obj: object) -> bool:
        return (
            isinstance(obj, dict)
            and (self.allow_empty or len(obj) > 0)
            and all(
                self.key_predicate.accept(key) and self.val_predicate.accept(val)
                for key, val in obj.items()
            )
        )

    def error_message(self, error_name: str) -> str:
        key_msg = self.key_predicate.error_message("each key")
        val_msg = self.val_predicate.error_message("each value")
        collection_name = "dictionary" if self.allow_empty else "non-empty dictionary"
        return f"{error_name} must be a {collection_name} where {key_msg} and {val_msg}"


class PossiblyEmptyMapOf(MapOf):
    """Predicate class to check that a dictionary with certain predicates for
    its keys and values, allowing empty dictionaries.
    """

    def __init__(
        self,
        key_predicate: PyDoughPredicate,
        val_predicate: PyDoughPredicate,
    ):
        super().__init__(key_predicate, val_predicate, True)


class NonEmptyMapOf(MapOf):
    """Predicate class to check that a dictionary with certain predicates for
    its keys and values, not allowing empty dictionaries.
    """

    def __init__(
        self,
        key_predicate: PyDoughPredicate,
        val_predicate: PyDoughPredicate,
    ):
        super().__init__(key_predicate, val_predicate, False)


class OrCondition(PyDoughPredicate):
    """Predicate class to check that an object is a list whose elements
    match one of several properties.
    """

    def __init__(self, predicates: list[PyDoughPredicate]):
        self.predicates: list[PyDoughPredicate] = predicates

    def accept(self, obj: object) -> bool:
        return any(predicate.accept(obj) for predicate in self.predicates)

    def error_message(self, error_name: str) -> str:
        combined_messages: str = " or ".join(
            predicate.error_message("it" if i > 0 else "")
            for i, predicate in enumerate(self.predicates)
        )
        return f"{error_name}{combined_messages}"


class PositiveInteger(PyDoughPredicate):
    """Predicate class to check that an object is a positive integer."""

    def accept(self, obj: object) -> bool:
        return isinstance(obj, int) and obj > 0

    def error_message(self, error_name: str) -> str:
        return f"{error_name} must be a positive integer"


###############################################################################
# Specific predicates
###############################################################################

is_valid_name: PyDoughPredicate = ValidName()
is_integer = HasType(int, "integer")
is_string = HasType(str, "string")
is_bool = HasType(bool, "boolean")
is_positive_int = PositiveInteger()
unique_properties_predicate: PyDoughPredicate = NonEmptyListOf(
    OrCondition([is_string, NonEmptyListOf(is_string)])
)
compound_relationship_inherited_predicate: PyDoughPredicate = PossiblyEmptyMapOf(
    is_string, is_string
)
simple_join_keys_predicate: PyDoughPredicate = NonEmptyMapOf(
    is_string, NonEmptyListOf(is_string)
)
