import requests
from batch_evaluate_response import BatchEvaluateResponse
from evaluate_request import BatchEvaluateRequest, EvaluateRequest
from evaluate_response import EvaluateResponse

__all__ = [
    "BatchEvaluateRequest",
    "BatchEvaluateResponse",
    "EvaluateRequest",
    "EvaluateResponse",
    "PredicateAPIClient",
]


class PredicateAPIClient:
    def __init__(self, base_url="http://localhost:3000/v1", token=None):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        self.session.headers.update({"Content-Type": "application/json"})

    def evaluate(self, request: EvaluateRequest) -> EvaluateResponse:
        url = f"{self.base_url}/predicates/evaluate"
        resp = self.session.post(url, json=request.to_dict())
        resp.raise_for_status()
        return EvaluateResponse(resp.json())

    def batch_evaluate(self, request: BatchEvaluateRequest) -> BatchEvaluateResponse:
        url = f"{self.base_url}/predicates/batch-evaluate"
        resp = self.session.post(url, json=request.to_dict())
        resp.raise_for_status()
        return BatchEvaluateResponse(resp.json())
