from fastapi import FastAPI
from pydantic import BaseModel

from .lookup_table import LOOKUP_TABLE

app: FastAPI = FastAPI()


class EvaluateRequest(BaseModel):
    column_reference: str
    predicate: list[str | int | float]
    mode: str = "dynamic"
    dry_run: bool = False


class RequestPayload(BaseModel):
    items: list[EvaluateRequest]
    expression_format: dict[str, str] = {"name": "linear", "version": "0.2.0"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/v1/predicates/batch-evaluate")
def batch_evaluate(request: RequestPayload):
    responses = []
    for item in request.items:
        key = (item.column_reference, tuple(item.predicate))
        response: dict = LOOKUP_TABLE.get(key, {"result": "UNSUPPORTED"})
        responses.append(response)

    return {"result": "SUCCESS", "items": responses}
