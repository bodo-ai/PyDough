"""
Mock FastAPI server for testing Mask Server interface.

This server provides endpoints to:
- Check server health with optional token authentication.
- Batch evaluate predicates using a lookup table for deterministic responses.

Intended for use in unit and integration tests.
"""

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

from .lookup_table import LOOKUP_TABLE

app: FastAPI = FastAPI()


class EvaluateRequest(BaseModel):
    column_ref: dict[str, str]
    predicate: list[str | int | float | None | bool]
    output_mode: str
    mode: str
    dry_run: bool


class RequestPayload(BaseModel):
    items: list[EvaluateRequest]
    expression_format: dict[str, str] = {"name": "linear", "version": "0.2.0"}


def verify_token(request: Request):
    auth_header = request.headers.get("Authorization", None)

    if auth_header and auth_header != "Bearer test-token-123":
        raise HTTPException(status_code=401, detail="Unauthorized request")

    return True


@app.get("/health")
def health(request: Request, authorized: bool = Depends(verify_token)):
    return {"status": "ok"}


@app.post("/v1/predicates/batch-evaluate")
def batch_evaluate(
    request: Request, payload: RequestPayload, authorized: bool = Depends(verify_token)
):
    responses: list[dict] = []
    for item in payload.items:
        assert set(item.column_ref.keys()) == {
            "kind",
            "value",
        }, f"Invalid column_reference format in mock: {item.column_ref!r}."
        assert item.column_ref["kind"] == "fqn", "Only FQN kind is supported in mock."
        key = (item.column_ref["value"], tuple(item.predicate))
        materialization: dict = LOOKUP_TABLE.get(key, {})

        response: dict = {
            "index": payload.items.index(item) + 1,
            "result": "SUCCESS" if materialization != {} else "UNSUPPORTED",
            "decision": {"strategy": "values", "reason": "mock"},
            "predicate_hash": "hash1",
            "encryption_mode": "clear",
            "materialization": materialization,
        }
        # Adding the index
        responses.append(response)

    return {"result": "SUCCESS", "items": responses}
