"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughCollectionAST"]


from abc import abstractmethod
from collections.abc import Iterable
from functools import cached_property
from typing import Union

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions.collation_expression import CollationExpression
from pydough.pydough_ast.expressions.expression_ast import PyDoughExpressionAST

from .collection_tree_form import CollectionTreeForm


class PyDoughCollectionAST(PyDoughAST):
    """
    The base class for AST nodes that represent a table collection accessed
    as a root.
    """

    def __repr__(self):
        return self.to_string()

    @property
    def ancestor_context(self) -> Union["PyDoughCollectionAST", None]:
        """
        The ancestor context from which this collection is derived, e.g. what
        is accessed by `BACK(1)`. Returns None if there is no ancestor context,
        e.g. because the collection is the top of the hierarchy.
        """

    @property
    @abstractmethod
    def preceding_context(self) -> Union["PyDoughCollectionAST", None]:
        """
        The preceding context from which this collection is derived, e.g. an
        ORDER BY term before a CALC. Returns None if there is no preceding
        context, e.g. because the collection is the start of a pipeline
        within a larger ancestor context.
        """

    @property
    @abstractmethod
    def calc_terms(self) -> set[str]:
        """
        The list of expressions that would be retrieved if the collection
        were to have its results evaluated. This is the set of names in the
        most-recent CALC, potentially with extra expressions added since then.
        """

    @property
    @abstractmethod
    def all_terms(self) -> set[str]:
        """
        The set of expression/subcollection names accessible by the context.
        """

    @abstractmethod
    def is_singular(self, context: "PyDoughCollectionAST") -> bool:
        """
        Returns whether the collection is singular with regards to a
        context collection.

        Args:
            `context`: the collection that the singular/plural status of the
            current collection is being checked against.

        Returns:
            True if there is at most a single record of the current collection
            for each record of the context, and False otherwise.
        """

    @cached_property
    def starting_predecessor(self) -> "PyDoughCollectionAST":
        """
        Returns the predecessor at the start of the current chain of preceding
        collections, or `self` if this is the start of that chain. The process
        also unwraps any ChildOperatorChildAccess terms.
        """
        from pydough.pydough_ast.collections import ChildOperatorChildAccess

        predecessor: PyDoughCollectionAST | None = self.preceding_context
        result: PyDoughCollectionAST
        if predecessor is None:
            result = self
        else:
            result = predecessor.starting_predecessor
        while isinstance(result, ChildOperatorChildAccess):
            result = result.child_access.starting_predecessor
        return result

    def verify_singular_terms(self, exprs: Iterable[PyDoughExpressionAST]) -> None:
        """
        Verifies that a list of expressions is singular with regards to the
        current collection, e.g. they can used as CALC terms.

        Args:
            `exprs`: the list of expression to be checked.

        Raises:
            `PyDoughASTException` if any element of `exprs` is not singular with
            regards to the current collection.
        """
        relative_context: PyDoughCollectionAST = self.starting_predecessor
        for expr in exprs:
            if not expr.is_singular(relative_context):
                raise PyDoughASTException(
                    f"Expected all terms in {self.standalone_string} to be singular, but encountered a plural expression: {expr.to_string()}"
                )

    @abstractmethod
    def get_expression_position(self, expr_name: str) -> int:
        """
        Retrieves the ordinal position of an expression within the collection
        if it were to be printed.

        Args:
            `expr_name`: the name of the expression that is having its ordinal
            position derived.

        Returns:
            The position that the expression would be in, if the collection
            were printed.

        Raises:
            `PyDoughASTException` if `expr_name` is not a name of one of the
            expressions in `calc_terms`.
        """

    @abstractmethod
    def get_term(self, term_name: str) -> PyDoughAST:
        """
        Obtains an expression or collection accessible from the current context
        by name.

        Args:
            `term_name`: the name of the term that is being extracted.


        Returns:
            `PyDoughASTException` if `term_name` is not a name of one of the
            terms accessible in the context.
        """

    def get_expr(self, term_name: str) -> PyDoughExpressionAST:
        """
        Obtains an expression accessible from the current context by name.

        Args:
            `term_name`: the name of the term that is being extracted.


        Returns:
            `PyDoughASTException` if `term_name` is not a name of one of the
            terms accessible in the context, or is not an expression.
        """
        term = self.get_term(term_name)
        if not isinstance(term, PyDoughExpressionAST):
            raise PyDoughASTException(
                f"Property {term_name!r} of {self} is not an expression"
            )
        return term

    def get_collection(self, term_name: str) -> "PyDoughCollectionAST":
        """
        Obtains a collection accessible from the current context by name.

        Args:
            `term_name`: the name of the term that is being extracted.


        Returns:
            `PyDoughASTException` if `term_name` is not a name of one of the
            terms accessible in the context, or is not a collection.
        """
        term = self.get_term(term_name)
        if not isinstance(term, PyDoughCollectionAST):
            raise PyDoughASTException(
                f"Property {term_name!r} of {self} is not a collection"
            )
        return term

    @property
    @abstractmethod
    def ordering(self) -> list[CollationExpression] | None:
        """
        Returns the ordering collation used by the collection, or None if it is
        unordered.
        """

    @property
    @abstractmethod
    def standalone_string(self) -> str:
        """
        The string representation of the node within `to_string` without any
        context of its predecessors/ancestors.
        """

    @abstractmethod
    def to_string(self) -> str:
        """
        Returns a PyDough collection AST converted to a simple string
        reminiscent of the original PyDough code.
        """

    @abstractmethod
    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        """
        Helper for `to_tree_form` that returns the `CollectionTreeForm` for
        the collection devoid of any information about its predecessors or
        ancestors.

        Args:
            `is_last`: boolean indicating if the current subtree is the last
            child of a ChildOperator node.
        """

    @abstractmethod
    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        """
        Helper for `to_tree_string` that turns a collection into a
        CollectionTreeForm object which can be used to create a tree string.

        Args:
            `is_last`: boolean indicating if the current subtree is the last
            child of a ChildOperator node.
        """

    @property
    @abstractmethod
    def tree_item_string(self) -> str:
        """
        The string representation of the node on the single line that becomes
        the `item_str` in its `CollectionTreeForm`.
        """

    def to_tree_string(self) -> str:
        """
        Returns a PyDough collection AST converted into a tree-like string,
        structured. For example, consider the following PyDough snippet:

        ```
        Regions.WHERE(ENDSWITH(name, 's')).nations.WHERE(name != 'USA')(
            a=BACK(1).name,
            b=name,
            c=MAX(YEAR(suppliers.WHERE(STARTSWITH(phone, '415')).supply_records.lines.ship_date)),
            d=COUNT(customers.WHERE(acctbal > 0))
        ).WHERE(
            c > 1000
        ).ORDER_BY(
            d.DESC()
        )
        ```

        A valid string representation of this would be:

        ```
        ──┬─ TPCH
          ├─── TableCollection[Regions]
          └─┬─ Where[ENDSWITH(name, 's')]
            ├─── SubCollection[nations]
            ├─── Where[name != 'USA']
            ├─┬─ Calc[a=[BACK(1).name], b=[name], c=[MAX($2._expr1)], d=[COUNT($1)]]
            │ ├─┬─ AccessChild
            │ │ ├─ SubCollection[customers]
            │ │ └─── Where[acctbal > 0]
            │ └─┬─ AccessChild
            │   └─┬─ SubCollection[suppliers]
            │     ├─── Where[STARTSWITH(phone, '415')]
            │     └─┬─ SubCollection[supply_records]
            │       └─┬─ SubCollection[lines]
            │         └─── Calc[_expr1=YEAR(ship_date)]
            ├─── Where[c > 1000]
            └─── OrderBy[d.DESC()]
        ```

        Returns:
            The tree-like string representation of `self`.
        """
        return "\n".join(self.to_tree_form(True).to_string_rows())
