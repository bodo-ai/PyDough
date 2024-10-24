"""
TODO: add file-level docstring
"""

from abc import ABC, abstractmethod

from typing import List

from pydough.pydough_ast import PyDoughAST


class TypeVerifier(ABC):
    """
    TODO: add class docstring
    """

    @abstractmethod
    def accepts(self, args: List[PyDoughAST]) -> None:
        """
        Verifies whether the type verifier accepts/rejects a list
        of arguments.

        Raises:
            `PyDoughASTException` if the arguments are rejected.
        """


class AllowAny(TypeVerifier):
    """
    TODO: add class docstring
    """

    def accepts(self, args: List[PyDoughAST]) -> None:
        pass
