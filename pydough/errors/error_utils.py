"""
The definitions of error-handling utilities used by PyDough
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
    "PyDoughPredicate",
    "extract_array",
    "extract_bool",
    "extract_integer",
    "extract_object",
    "extract_string",
    "find_possible_name_matches",
    "is_bool",
    "is_integer",
    "is_json_array",
    "is_json_object",
    "is_positive_int",
    "is_string",
    "is_valid_name",
    "is_valid_sql_name",
    "simple_join_keys_predicate",
    "unique_properties_predicate",
]


import builtins
import keyword
import re
from abc import ABC, abstractmethod

import numpy as np

from .error_types import PyDoughMetadataException

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

    def __init__(self):
        self.error_messages: dict[str, str] = {
            "identifier": "must be a string that is a valid Python identifier",
            "python_keyword": "must be a string that is not a Python reserved word or built-in name",
            "pydough_keyword": "must be a string that is not a PyDough reserved word",
            "sql_keyword": "must be a string that is not a SQL reserved word",
        }

    def _error_code(self, obj: object) -> str | None:
        """Return an error code if invalid, or None if valid."""
        ret_val: str | None = None
        # Check that obj is a string
        if isinstance(obj, str):
            # Check that obj is a valid Python identifier
            if not obj.isidentifier():
                ret_val = "identifier"
            # Check that obj is not a Python reserved word or built-in name
            elif keyword.iskeyword(obj) or hasattr(builtins, obj):
                ret_val = "python_keyword"
            # Check that obj is not a PyDough reserved word
            elif self._is_pydough_keyword(obj):
                ret_val = "pydough_keyword"
        else:
            ret_val = "identifier"

        return ret_val

    def _is_pydough_keyword(self, name: str) -> bool:
        """
        helper: Verifies if name is a PyDough reserved word.
                Extend with new PyDough reserved words if required.
        """
        # Dictionary of all registered operators pre-built from the PyDough source
        from pydough.pydough_operators import builtin_registered_operators

        # Set of collection operators
        PYDOUGH_RESERVED: set[str] = {
            "CALCULATE",
            "WHERE",
            "ORDER_BY",
            "TOP_K",
            "PARTITION",
            "SINGULAR",
            "BEST",
            "CROSS",
        }
        return (name in PYDOUGH_RESERVED) or (name in builtin_registered_operators())

    def accept(self, obj: object) -> bool:
        return self._error_code(obj) is None

    def error_message(self, error_name: str) -> str:
        # Generic fallback (since we don't have the object here)
        return f"{error_name} must be a valid identifier and not a reserved word"

    def verify(self, obj: object, error_name: str) -> None:
        code: str | None = self._error_code(obj)
        if code is not None:
            raise PyDoughMetadataException(f"{error_name} {self.error_messages[code]}")


class ValidSQLName(PyDoughPredicate):
    """Predicate class to check that an object is a string that can be used
    as the name for a SQL table path/column name.
    """

    # Regex for unquoted SQL identifiers
    _UNQUOTED_SQL_IDENTIFIER = re.compile(
        r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$"
    )

    def __init__(self):
        self.error_messages: dict[str, str] = {
            "identifier": "must have a SQL name that is a valid SQL identifier",
            "sql_keyword": "must have a SQL name that is not a reserved word",
        }

    def _error_code(self, obj: object) -> str | None:
        """Return an error code if invalid, or None if valid."""
        ret_val: str | None = None
        # Check that obj is a string
        if isinstance(obj, str):
            # Check that obj is a valid SQL identifier
            if not self.is_valid_sql_identifier(obj):
                ret_val = "identifier"
            # Check that obj is not a SQL reserved word
            elif self._is_sql_keyword(obj):
                ret_val = "sql_keyword"
        else:
            ret_val = "identifier"

        return ret_val

    def is_valid_sql_identifier(self, name: str) -> bool:
        """
        Check if a string is a valid SQL identifier.

        - Unquoted: starts with letter/underscore, then letters, digits,
            underscores.
        - Double-quoted: allows any chars, but " "" " is the only valid way to
            include a double-quote char.
        - Backtick-quoted: allows any chars, but `` `` `` is the only valid
            way to include a backtick char.
        """
        if not name:
            return False

        # Case 1: unquoted
        if self._UNQUOTED_SQL_IDENTIFIER.match(name):
            return True

        # Case 2: double quoted
        if name.startswith('"') and name.endswith('"'):
            inner = name[1:-1]
            # Any " must be escaped as ""
            return '"' not in inner.replace('""', "")

        # Case 3: backtick quoted
        if name.startswith("`") and name.endswith("`"):
            inner = name[1:-1]
            # Any ` must be escaped as ``
            return "`" not in inner.replace("``", "")

        return False

    # fmt: off
    SQL_RESERVED_KEYWORDS: set[str] = {
        # Query & DML
        "select", "from", "where", "group", "having", "distinct", "as", 
        "join", "inner", "union", "intersect", "except",

        # DDL & schema
        "create", "alter", "drop", "table", "view", "index", "sequence",
        "trigger", "schema", "database", "column", "constraint",

        # DML
        "insert", "update", "delete", "into", "values", "set",

        # Control flow & logical
        "and", "or", "not", "in", "is", "like", "between", "case", "when",
        "then", "else", "end", "exists",

        # Transaction & session
        "begin", "commit", "rollback", "savepoint", "transaction",
        "lock", "grant", "revoke",

        # Data types
        "int", "integer", "bigint", "smallint", "decimal", "numeric",
        "float", "real", "double", "char", "varchar", "text",
        "timestamp", "boolean", "null",

        # Functions
        "cast",
    }
    """
    Set of SQL reserved keywords that may cause conflicts when used as table or
    column names. This list was compiled from commonly reserved terms across
    multiple SQL dialects (e.g., PostgreSQL, SQLite, MySQL), with emphasis on
    keywords that are likely to appear in generated SQL statements.
    If any of these are used as identifiers, they must be properly escaped to
    avoid syntax errors.
    """
    # fmt: on

    def _is_sql_keyword(self, name: str) -> bool:
        """
        helper: Verifies if name is a SQL reserved word.
                Uses SQL_RESERVED_KEYWORDS set.
                Extend with new SQL reserved words if required.
        """
        return name.lower() in self.SQL_RESERVED_KEYWORDS

    def accept(self, obj: object) -> bool:
        return self._error_code(obj) is None

    def error_message(self, error_name: str) -> str:
        # Generic fallback (since we don't have the object here)
        return f"{error_name} must be a valid SQL identifier and not a reserved word"

    def verify(self, obj: object, error_name: str) -> None:
        code: str | None = self._error_code(obj)
        if code is not None:
            raise PyDoughMetadataException(f"{error_name} {self.error_messages[code]}")


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
is_valid_sql_name: PyDoughPredicate = ValidSQLName()
is_integer = HasType(int, "integer")
is_string = HasType(str, "string")
is_bool = HasType(bool, "boolean")
is_json_object = HasType(dict, "JSON object")
is_json_array = HasType(list, "JSON array")
is_positive_int = PositiveInteger()
unique_properties_predicate: PyDoughPredicate = NonEmptyListOf(
    OrCondition([is_string, NonEmptyListOf(is_string)])
)
simple_join_keys_predicate: PyDoughPredicate = NonEmptyMapOf(
    is_string, NonEmptyListOf(is_string)
)


################################################################################
# Extraction functions
################################################################################


def extract_string(json_obj: dict, key_name: str, obj_name: str) -> str:
    """
    Extracts a string field from a JSON object, returning the string field
    and verifying that the field exists and is well formed.

    Args:
        `json_obj`: the JSON object to extract the string from.
        `key_name`: the name of the key in the JSON object that
        contains the string.
        `obj_name`: the name of the object being extracted from, to be used
        in error messages.

    Returns:
        The string value of the field.

    Raises:
        `PyDoughMetadataException` if the JSON object does not contain a key
        with the name `key_name`, or if the value of the key is not a string.
    """
    HasPropertyWith(key_name, is_string).verify(json_obj, obj_name)
    value = json_obj[key_name]
    assert isinstance(value, str)
    return value


def extract_bool(json_obj: dict, key_name: str, obj_name: str) -> bool:
    """
    Extracts a boolean field from a JSON object, returning the string field
    and verifying that the field exists and is well formed.

    Args:
        `json_obj`: the JSON object to extract the string from.
        `key_name`: the name of the key in the JSON object that
        contains the boolean.
        `obj_name`: the name of the object being extracted from, to be used
        in error messages.

    Returns:
        The boolean value of the field.

    Raises:
        `PyDoughMetadataException` if the JSON object does not contain a key
        with the name `key_name`, or if the value of the key is not a boolean.
    """
    HasPropertyWith(key_name, is_bool).verify(json_obj, obj_name)
    value = json_obj[key_name]
    assert isinstance(value, bool)
    return value


def extract_integer(json_obj: dict, key_name: str, obj_name: str) -> int:
    """
    Extracts an integer field from a JSON object, returning the integer field
    and verifying that the field exists and is well formed.

    Args:
        `json_obj`: the JSON object to extract the string from.
        `key_name`: the name of the key in the JSON object that
        contains the string.
        `obj_name`: the name of the object being extracted from, to be used
        in error messages.

    Returns:
        The integer value of the field.

    Raises:
        `PyDoughMetadataException` if the JSON object does not contain a key
        with the name `key_name`, or if the value of the key is not an integer.
    """
    HasPropertyWith(key_name, is_integer).verify(json_obj, obj_name)
    value = json_obj[key_name]
    assert isinstance(value, int)
    return value


def extract_array(json_obj: dict, key_name: str, obj_name: str) -> list:
    """
    Extracts an array field from a JSON object, returning the string field
    and verifying that the field exists and is well formed.

    Args:
        `json_obj`: the JSON object to extract the string from.
        `key_name`: the name of the key in the JSON object that
        contains the array.
        `obj_name`: the name of the object being extracted from, to be used
        in error messages.

    Returns:
        A list containing the elements of the array.

    Raises:
        `PyDoughMetadataException` if the JSON object does not contain a key
        with the name `key_name`, or if the value of the key is not an array.
    """
    HasPropertyWith(key_name, is_json_array).verify(json_obj, obj_name)
    value = json_obj[key_name]
    assert isinstance(value, list)
    return value


def extract_object(json_obj: dict, key_name: str, obj_name: str) -> dict:
    """
    Extracts an object field from a JSON object, returning the string field
    and verifying that the field exists and is well formed.

    Args:
        `json_obj`: the JSON object to extract the string from.
        `key_name`: the name of the key in the JSON object that
        contains the object.
        `obj_name`: the name of the object being extracted from, to be used
        in error messages.

    Returns:
        A dictionary containing the elements of the object.

    Raises:
        `PyDoughMetadataException` if the JSON object does not contain a key
        with the name `key_name`, or if the value of the key is not a dictionary.
    """
    HasPropertyWith(key_name, is_json_object).verify(json_obj, obj_name)
    value = json_obj[key_name]
    assert isinstance(value, dict)
    return value


###############################################################################
# Name Suggestion Utilities
###############################################################################


def min_edit_distance(
    s: str,
    t: str,
    insert_cost: float,
    delete_cost: float,
    substitution_cost: float,
    capital_cost: float,
) -> float:
    """
    Computes the minimum edit distance between two strings using the
    Levenshtein distance algorithm. Substituting a character for the same
    character with different capitalization is considered 10% of the edit
    cost of replacing it with any other character. For this implementation
    the iterative with a 2-row array is used to save memory.
    Link:
    https://en.wikipedia.org/wiki/Levenshtein_distance#Iterative_with_two_matrix_rows

    Args:
        `s`: The first string.
        `t`: The second string.
        `insert_cost`: The cost of inserting a character into the first string.
        `delete_cost`: The cost of deleting a character from the first string.
        `substitution_cost`: The cost of substituting a character.
        `capital_cost`: The cost of substituting a character with the same
        character with different capitalization.

    Returns:
        The minimum edit distance between the two strings.
    """
    m, n = len(s), len(t)

    # Use a 2 x (m + 1) array to represent an n x (m + 1) array since you only
    # need to consider the previous row to generate the next row, therefore the
    # same two rows can be recycled

    row, previousRow = 1, 0
    arr = np.zeros((2, m + 1), dtype=float)

    # MED(X, "") = len(X)
    arr[0, :] = np.arange(m + 1)

    for i in range(1, n + 1):
        # MED("", X) = len(X)
        arr[row, 0] = i

        # Loop over the rest of s to see if it matches with the corresponding
        # letter of t
        for j in range(1, m + 1):
            sub_cost: float

            if s[j - 1] == t[i - 1]:
                sub_cost = 0.0
            elif s[j - 1].lower() == t[i - 1].lower():
                sub_cost = capital_cost
            else:
                sub_cost = substitution_cost

            arr[row, j] = min(
                arr[row, j - 1] + insert_cost,
                arr[previousRow, j] + delete_cost,
                arr[previousRow, j - 1] + sub_cost,
            )

        row, previousRow = previousRow, row

    return arr[previousRow, m]  # Return the last computed row's last element


def find_possible_name_matches(
    term_name: str,
    candidates: set[str],
    atol: int,
    rtol: float,
    min_names: int,
    max_names: int | None,
    insert_cost: float,
    delete_cost: float,
    substitution_cost: float,
    capital_cost: float,
) -> list[str]:
    """
    Finds and returns a list of candidate names that closely match the
    given name based on minimum edit distance.

    Args:
        `term_name`: The name to match against the list of candidates.
        `candidates`: A set of candidate names to search for matches.
        `atol`: The absolute tolerance for the minimum edit distance; any
        candidate with a minimum edit distance less than or equal to
        `closest_match + atol` will be included in the results.
        `rtol`: The relative tolerance for the minimum edit distance; any
            candidate with a minimum edit distance less than or equal to
        `closest_match * (1 + rtol)` will be included in the results.
        `min_names`: The minimum number of names to return.
        `max_names`: The maximum number of names to return. If None, there is
        no maximum.
        `insert_cost`: The cost of inserting a character into the first string.
        `delete_cost`: The cost of deleting a character from the first string.
        `substitution_cost`: The cost of substituting a character.
        `capital_cost`: The cost of substituting a character with the same
        character with different capitalization.

    Returns:
        A list of candidate names, based on the closest matches.
    """

    terms_distance_list: list[tuple[float, str]] = []

    for term in candidates:
        # get the minimum edit distance
        me: float = min_edit_distance(
            term_name, term, insert_cost, delete_cost, substitution_cost, capital_cost
        )
        terms_distance_list.append((me, term))

    if terms_distance_list == []:
        return []
    # sort the list by minimum edit distance break ties by name
    terms_distance_list.sort()

    closest_match = terms_distance_list[0]

    # List with all names that have a me <= closest_match + atol
    matches_within_atol: list[str] = [
        name for me, name in terms_distance_list if me <= closest_match[0] + atol
    ]

    # List with all names that have a me <= closest_match * 1.1
    matches_within_rtol: list[str] = [
        name for me, name in terms_distance_list if me <= closest_match[0] * (1 + rtol)
    ]

    # List with the top 3 closest matches (me) breaking ties by name
    min_matches: list[str] = [name for _, name in terms_distance_list[:min_names]]

    # Return whichever of the three lists is the longest, breaking ties
    # lexicographically by the names within. If a maximum number of names
    # is specified, truncate the list to that length.
    best_matches: list[str] = max(
        [matches_within_atol, matches_within_rtol, min_matches],
        key=lambda x: (len(x), x),
    )
    if max_names is not None:
        best_matches = best_matches[:max_names]
    return best_matches
