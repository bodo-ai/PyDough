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
    column_reference: str
    predicate: list[str | int | float | None | bool]
    mode: str = "dynamic"
    dry_run: bool = False


class RequestPayload(BaseModel):
    items: list[EvaluateRequest]
    expression_format: dict[str, str] = {"name": "linear", "version": "0.2.0"}


def authentication(request: Request):
    auth_header = request.headers.get("Authorization", None)
    api_key = request.headers.get("X-API-Key", None)

    if (auth_header and auth_header != "Bearer test-token-123") or (
        api_key and api_key != "api-key-123"
    ):
        raise HTTPException(status_code=401, detail="Unauthorized request")

    return True


@app.get("/health")
def health(request: Request, authorized: bool = Depends(authentication)):
    return {"status": "ok"}


@app.post("/v1/predicates/batch-evaluate")
def batch_evaluate(
    request: Request,
    payload: RequestPayload,
    authorized: bool = Depends(authentication),
):
    responses: list[dict] = []
    for item in payload.items:
        key = (item.column_reference, tuple(item.predicate))
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
