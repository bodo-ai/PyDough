import json
import re
from typing import Any


class PredicateParseError(Exception):
    """Raised when a predicate string/list does not match the EBNF structure."""


class PredicateExpression:
    """
    Linear, prefix-encoded predicate validator/printer based on stmt_format.ebnf.

    Build from JSON-like string or list:
        p1 = PredicateExpression.from_string('["EQUAL", 2, "YEAR", 1, "__col__", 2024]')
        p2 = PredicateExpression.from_list(["AND", 2, "GT", 2, "__col__", 10, "LT", 2, "__col__", 100])
    """

    OP_SPECS: dict[str, tuple[str, int]] = {
        # Logical
        "AND": ("min", 2),
        "OR": ("min", 2),
        "NOT": ("exact", 1),
        "COALESCE": ("min", 2),
        "IIF": ("exact", 3),
        # Comparison
        "EQUAL": ("exact", 2),
        "NOT_EQUAL": ("exact", 2),
        "LT": ("exact", 2),
        "LTE": ("exact", 2),
        "GT": ("exact", 2),
        "GTE": ("exact", 2),
        "LIKE": ("exact", 2),
        # Set membership
        "IN": ("min", 2),
        "NOT_IN": ("min", 2),
        # String/text
        "LOWER": ("exact", 1),
        "UPPER": ("exact", 1),
        "STARTSWITH": ("exact", 2),
        "ENDSWITH": ("exact", 2),
        "REGEXP": ("exact", 2),
        "CONTAINS": ("exact", 2),
        "SLICE": ("exact", 3),
        "CONCAT": ("min", 2),
        # Date/time
        "YEAR": ("exact", 1),
        "QUARTER": ("exact", 1),
        "MONTH": ("exact", 1),
        "DAY": ("exact", 1),
        "HOUR": ("exact", 1),
        "MINUTE": ("exact", 1),
        "SECOND": ("exact", 1),
        "DATETRUNC": ("exact", 2),
        "DATEADD": ("exact", 3),
        "DATEDIFF": ("exact", 3),
        # Range
        "BETWEEN": ("exact", 3),
        # Arithmetic
        "ADD": ("exact", 2),
        "SUB": ("exact", 2),
        "MUL": ("exact", 2),
        "DIV": ("exact", 2),
        "MOD": ("exact", 2),
        "ABS": ("exact", 1),
        "LEAST": ("min", 2),
        "GREATEST": ("min", 2),
        # Disambiguation of string literals
        "QUOTE": ("exact", 1),
    }

    COL_SPECIAL_RE = re.compile(r"^__col__(?:$)|^__col\d+__$")

    def __init__(
        self, items: list[Any], udf_registry: dict[str, tuple[str, int]] | None = None
    ):
        self.items: list[Any] = items
        self.udf_registry = udf_registry or {}
        self._validate()

    # -------------------------
    # Constructors
    # -------------------------

    @classmethod
    def from_list(
        cls, items: list[Any], udf_registry: dict[str, tuple[str, int]] | None = None
    ) -> "PredicateExpression":
        return cls(items, udf_registry=udf_registry)

    @classmethod
    def from_string(
        cls, text: str, udf_registry: dict[str, tuple[str, int]] | None = None
    ) -> "PredicateExpression":
        """
        Accepts only a JSON-like string (e.g. '["EQUAL", 2, "YEAR", 1, "__col__", 2024]').
        """
        try:
            arr = json.loads(text)
        except json.JSONDecodeError as e:
            raise PredicateParseError(f"Invalid JSON predicate string: {e}") from e

        if not isinstance(arr, list):
            raise PredicateParseError("Predicate must decode to a JSON array")

        return cls.from_list(arr, udf_registry=udf_registry)

    # -------------------------
    # Validation
    # -------------------------

    def _is_special_col(self, token: Any) -> bool:
        return isinstance(token, str) and bool(self.COL_SPECIAL_RE.match(token))

    def _operator_spec(self, op: str) -> tuple[str, int] | None:
        if op in self.OP_SPECS:
            return self.OP_SPECS[op]
        if op in self.udf_registry:
            return self.udf_registry[op]
        return None  # treat unknown UDF as allowing any arity

    def _validate(self) -> None:
        def walk(i: int) -> int:
            if i >= len(self.items):
                raise PredicateParseError("Unexpected end of input")

            token = self.items[i]
            is_op = (
                isinstance(token, str)
                and (i + 1) < len(self.items)
                and isinstance(self.items[i + 1], int)
            )

            if is_op:
                op = token
                arity = self.items[i + 1]
                if arity < 0:
                    raise PredicateParseError(f"Invalid negative arity for {op}")

                spec = self._operator_spec(op)
                if spec:
                    kind, n = spec
                    if kind == "exact" and arity != n:
                        raise PredicateParseError(
                            f"{op} expects arity {n}, got {arity}"
                        )
                    if kind == "min" and arity < n:
                        raise PredicateParseError(
                            f"{op} expects arity ≥ {n}, got {arity}"
                        )

                idx = i + 2
                for _ in range(arity):
                    idx = walk(idx)
                return idx

            if self._is_special_col(token):
                return i + 1

            return i + 1  # literal

        end = walk(0)
        if end != len(self.items):
            raise PredicateParseError("Extra tokens remaining after parsing")

    # -------------------------
    # Export & Pretty Print
    # -------------------------

    def to_list(self) -> list[Any]:
        return list(self.items)

    def to_string(self) -> str:
        return json.dumps(self.items)

    def pretty(self, indent: int = 0) -> str:
        def walk(i: int, level: int) -> tuple[str, int]:
            token = self.items[i]
            is_op = (
                isinstance(token, str)
                and (i + 1) < len(self.items)
                and isinstance(self.items[i + 1], int)
            )
            if is_op:
                op = token
                arity = self.items[i + 1]
                s = "  " * level + f"{op} (arity={arity})\n"
                idx = i + 2
                for _ in range(arity):
                    child_str, idx = walk(idx, level + 1)
                    s += child_str
                return s, idx
            else:
                return "  " * level + repr(token) + "\n", i + 1

        s, _ = walk(0, indent)
        return s

    def __repr__(self):
        return f"PredicateExpression({self.items})"
