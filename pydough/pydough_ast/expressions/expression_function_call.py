"""
Definition of PyDough AST nodes for function calls that return expressions.
"""

__all__ = ["ExpressionFunctionCall"]

from functools import cache

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.collections.collection_ast import PyDoughCollectionAST
from pydough.pydough_operators.expression_operators.expression_operator_ast import (
    PyDoughExpressionOperator,
)
from pydough.types import PyDoughType

from .expression_ast import PyDoughExpressionAST


class ExpressionFunctionCall(PyDoughExpressionAST):
    """
    The AST node implementation class representing a call to a function
    that returns an expression.
    """

    def __init__(
        self,
        operator: PyDoughExpressionOperator,
        args: list[PyDoughAST],
    ):
        operator.verify_allows_args(args)
        self._operator: PyDoughExpressionOperator = operator
        self._args: list[PyDoughAST] = args
        self._data_type: PyDoughType = operator.infer_return_type(args)

    @property
    def operator(self) -> PyDoughExpressionOperator:
        """
        The expression-returning PyDough operator corresponding to the
        function call.
        """
        return self._operator

    @property
    def args(self) -> list[PyDoughAST]:
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

    @cache
    def is_singular(self, context: PyDoughAST) -> bool:
        # Function calls are singular if they are aggregations or if all of
        # their operands are also singular.
        assert isinstance(context, PyDoughCollectionAST)
        if self.is_aggregation:
            return True
        for arg in self.args:
            if isinstance(
                arg, (PyDoughExpressionAST, PyDoughCollectionAST)
            ) and not arg.is_singular(context):
                return False
        return True

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        return self.operator.requires_enclosing_parens(parent)

    def to_string(self, tree_form: bool = False) -> str:
        from pydough.pydough_ast.collections.back_reference_collection import (
            BackReferenceCollection,
        )
        from pydough.pydough_ast.collections.child_reference_collection import (
            ChildReferenceCollection,
        )

        arg_strings: list[str] = []
        for arg in self.args:
            arg_string: str
            if isinstance(arg, PyDoughExpressionAST):
                arg_string = arg.to_string(tree_form)
                if arg.requires_enclosing_parens(self):
                    arg_string = f"({arg_string})"
            elif isinstance(arg, PyDoughCollectionAST):
                if tree_form:
                    assert isinstance(
                        arg, (ChildReferenceCollection, BackReferenceCollection)
                    ), f"Unexpected argument to function call {arg}: expected an expression, or reference to a collection"
                    arg_string = arg.tree_item_string
                else:
                    arg_string = arg.to_string()
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
