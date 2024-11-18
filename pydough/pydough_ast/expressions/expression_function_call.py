"""
TODO: add file-level docstring
"""

__all__ = ["ExpressionFunctionCall"]

from collections.abc import MutableSequence

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.collections.collection_ast import PyDoughCollectionAST
from pydough.pydough_ast.pydough_operators import PyDoughExpressionOperatorAST
from pydough.types import PyDoughType

from .expression_ast import PyDoughExpressionAST


class ExpressionFunctionCall(PyDoughExpressionAST):
    """
    The AST node implementation class representing a call to a function
    that returns an expression.
    """

    def __init__(
        self,
        operator: PyDoughExpressionOperatorAST,
        args: MutableSequence[PyDoughAST],
    ):
        operator.verify_allows_args(args)
        self._operator: PyDoughExpressionOperatorAST = operator
        self._args: MutableSequence[PyDoughAST] = args
        self._data_type: PyDoughType = operator.infer_return_type(args)

    @property
    def operator(self) -> PyDoughExpressionOperatorAST:
        """
        The expression-returning PyDough operator corresponding to the
        function call.
        """
        return self._operator

    @property
    def args(self) -> MutableSequence[PyDoughAST]:
        """
        The list of arguments to the function call.
        """
        return self._args

    @property
    def pydough_type(self) -> PyDoughType:
        return self._data_type

    @property
    def is_aggregation(self) -> bool:
        return self.operator.is_aggregation

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        return self.operator.requires_enclosing_parens(parent)

    def to_string(self, tree_form: bool = False) -> str:
        arg_strings: list[str] = []
        for arg in self.args:
            arg_string: str
            if isinstance(arg, PyDoughExpressionAST):
                arg_string = arg.to_string(tree_form)
                if arg.requires_enclosing_parens(self):
                    arg_string = f"({arg_string})"
            elif isinstance(arg, PyDoughCollectionAST):
                arg_string = arg.tree_item_string
            else:
                arg_string = str(arg)
            arg_strings.append(arg_string)
        return self.operator.to_string(arg_strings)

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ExpressionFunctionCall)
            and (self.operator == other.operator)
            and (self.args == other.args)
        )
