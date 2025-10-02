"""
Interface for the mask server. This API includes the MaskServerInfo class and related
data structures including the MaskServerInput and MaskServerOutput dataclasses.
"""

__all__ = [
    "MaskServerInfo",
    "MaskServerInput",
    "MaskServerOutput",
    "MaskServerResponse",
]

from dataclasses import dataclass
from enum import Enum
from typing import Any

from pydough.mask_server.server_connection import (
    RequestMethod,
    ServerConnection,
    ServerRequest,
)


class MaskServerResponse(Enum):
    """
    Enum to represent the type of response from the MaskServer.
    """

    IN_ARRAY = "IN_ARRAY"
    """
    The mask server returned an "IN" response.
    """

    NOT_IN_ARRAY = "NOT_IN_ARRAY"
    """
    The mask server returned an "NOT_IN" response.
    """

    UNSUPPORTED = "UNSUPPORTED"
    """
    The mask server returned an "UNSUPPORTED" response. Or the response is not 
    one of the supported cases.
    """


@dataclass
class MaskServerInput:
    """
    Input data structure for the MaskServer.
    """

    table_path: str
    """
    The fully qualified SQL table path, given from the metadata.
    """

    column_name: str
    """
    The SQL column name, given from the metadata.
    """

    expression: list[str | int | float | None | bool]
    """
    The linear serialization of the predicate expression.
    """


@dataclass
class MaskServerOutput:
    """
    Output data structure for the MaskServer.

    If the server returns an unsupported value, it returns an output with
    UNSUPPORTED + a None payload.
    """

    response_case: MaskServerResponse
    """
    The type of response from the server.
    """

    payload: Any
    """
    The payload of the response. This can be the result of the predicate evaluation
    or None if an error occurred.
    """


class MaskServerInfo:
    """
    The MaskServeraInfo class is responsible for evaluating predicates against a
    given table and column. It interacts with an external mask server to
    perform the evaluation.
    """

    def __init__(self, base_url: str, token: str | None = None):
        """
        Initialize the MaskServerInfo with the given server URL.

        Args:
            `base_url`: The URL of the mask server.
            `token`: Optional authentication token for the server.
        """
        self.connection: ServerConnection = ServerConnection(
            base_url=base_url, token=token
        )

    def get_server_response_case(self, server_case: str) -> MaskServerResponse:
        """
        Mapping from server response strings to MaskServerResponse enum values.

        Args:
            `server_case`: The response string from the server.
        Returns:
            The corresponding MaskServerResponse enum value.
        """
        match server_case:
            case "IN":
                return MaskServerResponse.IN_ARRAY
            case "NOT_IN":
                return MaskServerResponse.NOT_IN_ARRAY
            case _:
                return MaskServerResponse.UNSUPPORTED

    def simplify_simple_expression_batch(
        self, batch: list[MaskServerInput]
    ) -> list[MaskServerOutput]:
        """
        Sends a batch of predicate expressions to the mask server for evaluation.

        Each input in the batch specifies a table, column, and predicate
        expression.The method constructs a request, sends it to the server,
        and parses the response into a list of MaskServerOutput objects, each
        indicating the server's decision for the corresponding input.

        Args:
            `batch`: The list of inputs to be sent to the server.

        Returns:
            An output list containing the response case and payload.
        """
        assert batch != [], "Batch cannot be empty."

        path: str = "v1/predicates/batch-evaluate"
        method: RequestMethod = RequestMethod.POST

        request: ServerRequest = self.generate_request(batch, path, method)

        response_json = self.connection.send_server_request(request)
        result: list[MaskServerOutput] = self.generate_result(response_json)

        return result

    def generate_request(
        self, batch: list[MaskServerInput], path: str, method: RequestMethod
    ) -> ServerRequest:
        """
        Generate a server request from the given batch of server inputs and path.

        Args:
            `batch`: A list of MaskServerInput objects.
            `path`: The API endpoint path.

        Returns:
            A server request including payload to be sent.

        Example payload:
        {
            "items": [
                {
                    "column_reference": "srv.db.tbl.col",
                    "predicate": ["EQUAL", 2, "__col__", 1],
                    "mode": "dynamic",
                    "dry_run": false
                },
                ...
            ],
            "expression_format": {"name": "linear", "version": "0.2.0"}
        }
        """

        payload: dict = {
            "items": [],
            "expression_format": {"name": "linear", "version": "0.2.0"},
        }

        for item in batch:
            evaluate_request: dict = {
                "column_reference": f"{item.table_path}.{item.column_name}",
                "predicate": item.expression,
                "mode": "dynamic",
                "dry_run": False,
            }
            payload["items"].append(evaluate_request)

        return ServerRequest(path=path, payload=payload, method=method)

    def generate_result(self, response: dict) -> list[MaskServerOutput]:
        """
        Generate a list of server outputs from the server response.

        Args:
            `response`: The response from the mask server.

        Returns:
            A list of server outputs objects.

        Example response:
        {
            "result": "SUCCESS",
            "items": [
                {
                    "index": 0,
                    "result": "SUCCESS",
                    "decision": {"strategy": "values", "reason": "mock"},
                    "predicate_hash": "hash0",
                    "encryption_mode": "clear",
                    "materialization": {
                        "type": "literal",
                        "operator": "IN",
                        "values": [0],
                        "count": 1
                    }
                },
                ...
            ]
        }
        """
        result: list[MaskServerOutput] = []

        for item in response.get("items", []):
            """
            Case on whether operator is ERROR or not
                If ERROR, then response_case is unsupported and payload is None
                Otherwise, call self.get_server_response(operator) to get the enum, store in a variable, then case on this variable to obtain the payload (use item.get("materialization", {}).get("values", []) if it is IN_ARRAY or NOT_IN_ARRAY, otherwise None)
            """
            if item.get("result") == "ERROR":
                result.append(
                    MaskServerOutput(
                        response_case=MaskServerResponse.UNSUPPORTED,
                        payload=None,
                    )
                )
            else:
                materialization: dict = item.get("materialization", {})
                response_case: MaskServerResponse = self.get_server_response_case(
                    materialization.get("operator", "ERROR")
                )

                payload: Any = None

                if response_case in (
                    MaskServerResponse.IN_ARRAY,
                    MaskServerResponse.NOT_IN_ARRAY,
                ):
                    payload = materialization.get("values", [])

                result.append(
                    MaskServerOutput(
                        response_case=response_case,
                        payload=payload,
                    )
                )

        return result
