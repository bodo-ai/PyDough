from typing import Any

from evaluate_response import EvaluateResponse

__all__ = [
    "BatchEvaluateItemResponse",
    "BatchEvaluateResponse",
]


class BatchEvaluateItemResponse(EvaluateResponse):
    """
    Represents a single item in a BatchEvaluateResponse.
    Inherits from EvaluateResponse and adds the 'index' field.
    """

    def __init__(self, data: dict[str, Any]):
        self.index: int = data.get("index", -1)
        super().__init__(data)

    def to_dict(self) -> dict[str, Any]:
        base = super().to_dict()
        base["index"] = self.index
        return base

    def __repr__(self):
        return f"BatchEvaluateItemResponse(index={self.index}, result={self.result})"


class BatchEvaluateResponse:
    """
    Represents the overall BatchEvaluateResponse.
    """

    def __init__(self, data: dict[str, Any]):
        self.result: str | None = data.get("result")
        self.items: list[BatchEvaluateItemResponse] = [
            BatchEvaluateItemResponse(item) for item in data.get("items", [])
        ]
        self._raw = data

    def to_dict(self) -> dict[str, Any]:
        return {
            "result": self.result,
            "items": [item.to_dict() for item in self.items],
        }

    def __iter__(self):
        return iter(self.items)

    def __len__(self):
        return len(self.items)

    def __repr__(self):
        return f"BatchEvaluateResponse(result={self.result}, items={len(self.items)})"
