"""
Interface for the mask server. This API includes the MaskServer class and related
data structures. Also includes MaskServerInput and MaskServerOutput dataclasses.
"""

__all__ = ["MaskServer", "MaskServerInput", "MaskServerOutput"]

from dataclasses import dataclass
from enum import Enum
from typing import Any

from pydough.mask_server.server_connection import (
    RequestMethod,
    ServerConnection,
    ServerRequest,
)


class MaskServerReponse(Enum):
    """
    Enum to represent the type of response from the MaskServer.
    """

    IN_ARRAY = "IN_ARRAY"
    """
    TODO
    """

    NOT_IN_ARRAY = "NOT_IN_ARRAY"
    """
    TODO
    """

    UNSUPPORTED = "UNSUPPORTED"
    """
    An error occurred during predicate evaluation.
    """


@dataclass
class MaskServerInput:
    """
    Input data structure for the MaskServer.
    """

    table_path: str
    """
    The fully qualified SQL table path, which we get from the metadata.
    """

    column_name: str
    """
    The SQL column name, which we get from the metadata.
    """

    expression: list[str | int | float]
    """"
    The linear serialization of the predicate expression.
    """


@dataclass
class MaskServerOutput:
    """
    Output data structure for the MaskServer.

    If the value returned from the server is not one of our supported cases,
    we return an output with UNSUPPORTED + a None payload
    """

    response_case: MaskServerReponse
    """
    The type of response from the server.
    """

    payload: Any
    """
    The payload of the response. This can be the result of the predicate evaluation
    or None if an error ocurred.
    """


class MaskServer:
    """
    The MaskServer class is responsible for evaluating predicates against a
    given table and column. It interacts with an external mask server to
    perform the evaluation.
    """

    def __init__(self, base_url: str, token: str | None = None):
        """
        Initialize the MaskServer with the given server URL.

        Args:
            `server_url`: The URL of the mask server.
        """
        self.connection = ServerConnection(base_url=base_url, token=token)

    def get_server_response(self, server_case: str) -> MaskServerReponse:
        """
        Mapping from server response strings to MaskServerReponse enum values.
        """
        match server_case:
            case "IN":
                return MaskServerReponse.IN_ARRAY
            case "NOT_IN":
                return MaskServerReponse.NOT_IN_ARRAY
            case _:
                return MaskServerReponse.UNSUPPORTED

    def simplify_simple_expression_batch(
        self, batch: list[MaskServerInput], path: str, method: RequestMethod
    ) -> list[MaskServerOutput]:
        """
        Evaluate the predicate expression against the specified table and column.

        Args:
            `input_data`: The input data containing table path, column name, and expression.

        Returns:
            A MaskServerOutput containing the response case and payload.
        """
        request: ServerRequest = self.generate_request(batch, path, method)

        response_json = self.connection.send_server_request(request)
        result: list[MaskServerOutput] = self.generate_result(response_json)

        return result

    def generate_request(
        self, batch: list[MaskServerInput], path: str, method: RequestMethod
    ) -> ServerRequest:
        """
        Generate a ServerRequest object for the given batch of MaskServerInput.
        Args:
            `batch`: A list of MaskServerInput objects.
            `path`: The API endpoint path.

        Returns:
            A ServerRequest including payload to be sent.
        """

        payload: dict = {
            "items": [],
            "expression_format": {"name": "linear", "version": "0.2.0"},
        }

        for item in batch:
            evalute_request: dict = {
                "column_reference": f"{item.table_path}.{item.column_name}",
                "predicate": item.expression,
                "mode": "dynamic",
                "dry_run": False,
            }
            payload["items"].append(evalute_request)

        return ServerRequest(path=path, payload=payload, method=method)

    def generate_result(self, response: dict) -> list[MaskServerOutput]:
        """
        Generate a list of MaskServerOutput from the server response.

        Args:
            `response`: The response from the mask server.

        Returns:
            A list of MaskServerOutput objects.
        """
        result: list[MaskServerOutput] = []

        for item in response.get("items", []):
            if item.get("result") == "SUCCESS":
                operator: str = item.get("materialization", {}).get("operator", "")

                result.append(
                    MaskServerOutput(
                        response_case=self.get_server_response(operator),
                        payload=item.get("materialization", {}).get("values", []),
                    )
                )
            else:
                result.append(
                    MaskServerOutput(
                        response_case=MaskServerReponse.UNSUPPORTED,
                        payload=None,
                    )
                )

        return result
