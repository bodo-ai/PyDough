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

import sqlglot.expressions as exp
from sqlglot import parse_one

from pydough.logger import get_logger
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

    def fully_qualified_name(self) -> str:
        """
        Returns the fully qualified name of the column in the format
        'table_path/column_name', with `/` as the separator used to modify the
        `table_path` appropriately.
        """
        table_path_chunks: list[str] = []
        parsed: exp.Expression = parse_one(self.table_path, dialect="mysql")
        self.dump_identifier_chunks(parsed, table_path_chunks)
        return f"{'/'.join(table_path_chunks)}/{self.column_name}"

    def dump_identifier_chunks(
        self,
        expression: exp.Expression,
        chunks: list[str],
    ) -> None:
        """
        Recursively dumps the identifier chunks from the parsed SQL expression.

        Args:
            `expression`: The parsed SQL expression.
            `chunks`: The list to append the identifier chunks to.
        """
        match expression:
            case exp.Identifier():
                chunks.append(expression.sql())
            case exp.Literal() if expression.is_string:
                chunks.append(expression.sql())
            case exp.Column() | exp.Dot():
                for part in expression.parts:
                    self.dump_identifier_chunks(part, chunks)
            case _:
                raise ValueError(
                    f"Unexpected expression type in table path parse tree: {expression.__class__.__name__}"
                )


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
    The MaskServerInfo class is responsible for evaluating predicates against a
    given table and column. It interacts with an external mask server to
    perform the evaluation.
    """

    def __init__(self, base_url: str, server_address: str, token: str | None = None):
        """
        Initialize the MaskServerInfo with the given server URL.

        Args:
            `base_url`: The URL of the mask server.
            `server_address`: The server address to place at the front of all
            qualified table paths.
            `token`: Optional authentication token for the server.
        """
        self.connection: ServerConnection = ServerConnection(
            base_url=base_url, token=token
        )
        self.server_address: str = server_address

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
        self,
        batch: list[MaskServerInput],
        dry_run: bool,
        hard_limit: int,
    ) -> list[MaskServerOutput]:
        """
        Sends a batch of predicate expressions to the mask server for evaluation.

        Each input in the batch specifies a table, column, and predicate
        expression.The method constructs a request, sends it to the server,
        and parses the response into a list of MaskServerOutput objects, each
        indicating the server's decision for the corresponding input.

        Args:
            `batch`: The list of inputs to be sent to the server.
            `dry_run`: Whether to perform a dry run or not.
            `hard_limit`: The maximum number of items that can be returned for
            each predicate.

        Returns:
            An output list containing the response case and payload.
        """

        # Log the batch request
        pyd_logger = get_logger(__name__)
        if dry_run:
            pyd_logger.info(
                f"Batch request (dry run) to Mask Server ({len(batch)} items):"
            )
        else:
            pyd_logger.info(f"Batch request to Mask Server ({len(batch)} items):")
        for idx, item in enumerate(batch):
            pyd_logger.info(
                f"({idx + 1}) {item.fully_qualified_name}: {item.expression}"
            )

        assert batch != [], "Batch cannot be empty."

        path: str = "v1/predicates/batch-evaluate"
        method: RequestMethod = RequestMethod.POST
        request: ServerRequest = self.generate_request(
            batch, path, method, dry_run, hard_limit
        )
        response_json = self.connection.send_server_request(request)
        result: list[MaskServerOutput] = self.generate_result(response_json)

        return result

    def generate_request(
        self,
        batch: list[MaskServerInput],
        path: str,
        method: RequestMethod,
        dry_run: bool,
        hard_limit: int,
    ) -> ServerRequest:
        """
        Generate a server request from the given batch of server inputs and path.

        Args:
            `batch`: A list of MaskServerInput objects.
            `path`: The server path for the request.
            `method`: The HTTP method for the request.
            `dry_run`: Whether the request is a dry run or not.
            `hard_limit`: The maximum number of items that can be returned for
            each predicate.

        Returns:
            A server request including payload to be sent.

        Example payload:
        ```
        {
            "items": [
                {
                    "column_ref": {"kind": "fqn", "value": "srv.db.schema.table.name"},
                    "predicate": ["EQUAL", 2, "__col__", 1],
                    "mode": "dynamic",
                    "predicate_format": "linear_with_arity",
                    "output_mode": "cell_encrypted",
                    "dry_run": true,
                },
                ...
            ],
            "expression_format": {"name": "linear", "version": "0.2.0"}
            "hard_limit": 1000,
        }
        ```
        """

        payload: dict = {
            "items": [],
            "expression_format": {"name": "linear", "version": "0.2.0"},
            "hard_limit": hard_limit,
        }

        for item in batch:
            evaluate_request: dict = {
                "column_ref": {
                    "kind": "fqn",
                    "value": f"{self.server_address}/{item.fully_qualified_name}",
                },
                "predicate": item.expression,
                "output_mode": "cell_encrypted",
                "mode": "dynamic",
                "dry_run": dry_run,
            }
            payload["items"].append(evaluate_request)

        return ServerRequest(path=path, payload=payload, method=method)

    def generate_result(self, response_dict: dict) -> list[MaskServerOutput]:
        """
        Generate a list of server outputs from the server response of a
        non-dry-run request.

        Args:
            `response_dict`: The response from the mask server.

        Example response:
        ```
        {
            "result": "SUCCESS",
            "items": [
                {
                    "index": 0,
                    "result": "SUCCESS",
                    "response": {
                        "strategy": ...,

                        "records": [
                            {
                                "mode": "cell_encrypted",
                                "cell_encrypted": "abcE1dsa",
                            }
                        ],

                        "count": ...,

                        "stats": ...,

                        "column_stats": ...,

                        "next_cursor": ...,

                        "metadata": {
                            "dynamic_operator": "IN",
                            ...
                        }
                    }
                },
                ...
            ]
        }
        ```

        Returns:
            A list of server outputs objects.
        """
        result: list[MaskServerOutput] = []

        for item in response_dict.get("items", []):
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
                response: dict = item.get("response", None)
                if response is None:
                    # In this case, use a dummy value as a default to indicate
                    # the dry run was successful
                    result.append(
                        MaskServerOutput(
                            response_case=MaskServerResponse.IN_ARRAY,
                            payload=None,
                        )
                    )
                else:
                    response_case: MaskServerResponse = self.get_server_response_case(
                        response["metadata"]["dynamic_operator"]
                    )

                    payload: Any = None

                    if response_case in (
                        MaskServerResponse.IN_ARRAY,
                        MaskServerResponse.NOT_IN_ARRAY,
                    ):
                        payload = [
                            record.get("cell_encrypted")
                            for record in response.get("records", [])
                        ]

                    result.append(
                        MaskServerOutput(
                            response_case=response_case,
                            payload=payload,
                        )
                    )

        return result
