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


def verify_token(request: Request):
    auth_header = request.headers.get("Authorization", None)
    if auth_header:
        if auth_header != "Bearer test-token-123":
            raise HTTPException(status_code=401, detail="Unauthorized request")
    return True


@app.get("/health")
def health(request: Request, authorized: bool = Depends(verify_token)):
    return {"status": "ok"}


@app.post("/v1/predicates/batch-evaluate")
def batch_evaluate(
    request: Request, payload: RequestPayload, authorized: bool = Depends(verify_token)
):
    responses = []
    for item in payload.items:
        key = (item.column_reference, tuple(item.predicate))
        response: dict = LOOKUP_TABLE.get(key, {"result": "UNSUPPORTED"})
        # Adding the index
        response["index"] = payload.items.index(item) + 1
        responses.append(response)

    return {"result": "SUCCESS", "items": responses}
