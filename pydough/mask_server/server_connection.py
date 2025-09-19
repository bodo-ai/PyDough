"""
Connection module for the mask server. It contains the data structures
and the main class to interact with the mask server.
"""

__all__ = ["RequestMethod", "ServerConnection", "ServerRequest"]

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

import httpx
from httpx import Response


class RequestMethod(Enum):
    """
    Class to represent the type of request to the MaskServer.
    """

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


@dataclass
class ServerRequest:
    """
    Dataclass for the request of the MaskServer.
    """

    path: str = ""
    """
    The path to the endpoint, e.g. /v1/predicates/evaluate
    """

    payload: dict = field(default_factory=dict)
    """
    The payload of the request.
    """

    method: RequestMethod = RequestMethod.GET
    """
    The HTTP method to use for the request.
    """

    headers: dict = field(default_factory=lambda: {"Content-Type": "application/json"})
    """
    Optional headers to include in the request.
    """


class ServerConnection:
    """
    Class to manage the connection to the mask server.
    """

    def __init__(self, base_url: str, token: str | None = None):
        """
        Initialize the MaskServerConnection with the given server URL.

        Args:
            `base_url`: The URL of the mask server.
            `token`: The Token to use for authentication.
        """
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._client = httpx.Client(base_url=self.base_url)

    def method_mapping(self, request_method: RequestMethod) -> Callable[..., Response]:
        """
        Map of endpoints to their request methods.

        Returns:
            A dictionary mapping endpoint strings to RequestMethod enums.
        """
        match request_method:
            case RequestMethod.GET:
                return self._client.get
            case RequestMethod.POST:
                return self._client.post
            case RequestMethod.PUT:
                return self._client.put
            case RequestMethod.DELETE:
                return self._client.delete
            case _:
                raise ValueError(f"Unsupported request method: {request_method}")

    def send_server_request(self, request: ServerRequest) -> dict:
        """
        Send a request to the mask server.

        Args:
            `request`: The request to send to the server.

        Returns:
            The response from the server as a dictionary.
        """

        try:
            method: Callable[..., Response] = self.method_mapping(request.method)
            request_url: str = f"{self.base_url}{request.path}"

            # choose params vs json depending on method
            headers = request.headers.copy() if request.headers else {}
            if self.token:
                headers.setdefault("Authorization", f"Bearer {self.token}")

            kwargs = {"headers": headers}
            if request.method in (RequestMethod.GET, RequestMethod.DELETE):
                kwargs["params"] = request.payload
            else:  # POST, PUT
                kwargs["json"] = request.payload

            print(f"Making {request.method} request to {request_url} with {kwargs}")
            response: Response = method(request.path, **kwargs)
            return response.json()

        except httpx.RequestError as e:
            return {"error": f"Request failed: {e}"}
        except httpx.HTTPStatusError as e:
            return {
                "error": f"Bad response {e.response.status_code}: {e.response.text}"
            }
