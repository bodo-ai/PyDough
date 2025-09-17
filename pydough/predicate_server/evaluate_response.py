# -------------------------------
# Base class
# -------------------------------


class Materialization:
    """
    Abstract base class for materialization responses.
    """

    def __init__(self, type_: str):
        self.type: str = type_

    def to_dict(self) -> dict:
        raise NotImplementedError("Subclasses must implement to_dict()")

    def __repr__(self):
        return f"{self.__class__.__name__}(type={self.type})"


# -------------------------------
# Literal materialization
# -------------------------------


class MaterializationLiteral(Materialization):
    def __init__(self, data: dict):
        super().__init__("literal")
        self.operator: str = data.get("operator", "IN")
        self.values: list = data.get("values", [])
        self.count: int = data.get("count", len(self.values))

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "operator": self.operator,
            "values": self.values,
            "count": self.count,
        }

    def __repr__(self):
        return f"MaterializationLiteral(operator={self.operator}, values={self.values}, count={self.count})"


# -------------------------------
# Row IDs materialization
# -------------------------------


class MaterializationRowIds(Materialization):
    def __init__(self, data: dict):
        super().__init__("row_ids")
        self.row_ids: list[str] = data.get("row_ids", [])
        self.count: int = data.get("count", len(self.row_ids))

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "row_ids": self.row_ids,
            "count": self.count,
        }

    def __repr__(self):
        return f"MaterializationRowIds(row_ids={self.row_ids}, count={self.count})"


# -------------------------------
# EvaluateResponse
# -------------------------------


class EvaluateResponse:
    """
    Represents the response from /v1/predicates/evaluate.
    Focuses on core fields + encryption_mode.
    """

    def __init__(self, data: dict):
        self.result: str | None = data.get("result")
        self.decision: dict = data.get("decision", {})
        self.predicate_hash: str | None = data.get("predicate_hash")
        self.encryption_mode: str | None = data.get("encryption_mode")

        mat = data.get("materialization")
        if isinstance(mat, dict):
            if mat.get("type") == "literal":
                self.materialization: Materialization | None = MaterializationLiteral(
                    mat
                )
            elif mat.get("type") == "row_ids":
                self.materialization = MaterializationRowIds(mat)
            else:
                self.materialization = None
        else:
            self.materialization = None

        self._raw = data

    def to_dict(self) -> dict:
        base: dict = {
            "result": self.result,
            "decision": self.decision,
            "predicate_hash": self.predicate_hash,
            "encryption_mode": self.encryption_mode,
        }
        if self.materialization:
            base["materialization"] = self.materialization.to_dict()
        return base

    def __repr__(self):
        return f"EvaluateResponse(result={self.result}, strategy={self.decision.get('strategy')})"
