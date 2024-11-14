"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughCollectionAST"]


from abc import abstractmethod
from typing import Union

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
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
        assert isinstance(term, PyDoughExpressionAST)
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
        assert isinstance(term, PyDoughCollectionAST)
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
    def to_tree_form_isolated(self) -> CollectionTreeForm:
        """
        Helper for `to_tree_form` that returns the `CollectionTreeForm` for
        the collection devoid of any information about its predecessors or
        ancestors.
        """

    @abstractmethod
    def to_tree_form(self) -> CollectionTreeForm:
        """
        Helper for `to_tree_string` that turns a collection into a
        CollectionTreeForm object which can be used to create a tree string.
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
        return "\n".join(self.to_tree_form().to_string_rows())
