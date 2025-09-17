from typing import Any

from predicate_expression import PredicateExpression

DEFAULT_EXPR_FMT = {
    "name": "linear",
    "version": "0.2.0",
}

# -------------------------------
# Request Classes
# -------------------------------


class EvaluateRequest:
    """
    Represents a minimal EvaluateRequest for the Predicate API.

    Only supports:
      - column_reference (string)
      - predicate (PredicateExpression)
      - mode (default: "dynamic")
      - dry_run (default: False)
      - expression_format (default: { "name": "linear", "version": "0.2.0" })
    """

    def __init__(
        self,
        column_reference: str,
        predicate: PredicateExpression,
        mode: str = "dynamic",
        dry_run: bool = False,
        expression_format: dict[str, str] = DEFAULT_EXPR_FMT,
    ):
        self.column_reference: str = column_reference
        self.predicate: PredicateExpression = predicate
        self.mode: str = mode
        self.dry_run: bool = dry_run
        self.expression_format: dict[str, str] = expression_format

    def to_dict(self) -> dict:
        return {
            "column_reference": self.column_reference,
            "predicate": self.predicate.to_list(),
            "mode": self.mode,
            "dry_run": self.dry_run,
            "expression_format": self.expression_format,
        }

    def __repr__(self):
        return f"EvaluateRequest(column_reference={self.column_reference}, predicate={self.predicate})"


class BatchEvaluateRequest:
    """
    Holds multiple EvaluateRequest items.
    """

    def __init__(self, expression_format: dict[str, Any] | None = None):
        self.items: list[EvaluateRequest] = []
        self.expression_format = expression_format

    def add_item(self, evaluate_request: EvaluateRequest):
        self.items.append(evaluate_request)

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"items": [item.to_dict() for item in self.items]}
        if self.expression_format:
            data["expression_format"] = self.expression_format
        return data

    def __len__(self):
        return len(self.items)

    def __repr__(self):
        return f"BatchEvaluateRequest(items={len(self.items)}, expression_format={self.expression_format})"
