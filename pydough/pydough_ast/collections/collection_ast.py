"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughCollectionAST"]

from abc import abstractmethod

from typing import Set, Union

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST


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
    def calc_terms(self) -> Set[str]:
        """
        The list of expressions that would be retrieved if the collection
        were to be printed. This is the equivalent of the most-recent CALC
        term.
        """

    @property
    @abstractmethod
    def all_terms(self) -> Set[str]:
        """
        The set of each expression name accessible by the context.
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
        Obtains an expression accessible from the current context by name.

        Args:
            `term_name`: the name of the term that is being extracted.


        Returns:
            `PyDoughASTException` if `term_name` is not a name of one of the
            terms accessible in the context.
        """

    @abstractmethod
    def to_string(self) -> str:
        """
        Returns a PyDough collection AST converted to a simple string
        reminiscent of the original PyDough code.
        """

    @abstractmethod
    def to_tree_string(self) -> str:
        """
        Returns a PyDough collection AST converted into a tree-like string,
        structured. For example, consider the following PyDough snippet:

        ```
        Regions.WHERE(STARTSWITH(name, 'A')).nations(
            a=BACK(1).name,
            b=name,
            c=MAX(suppliers.supply_records.lines.ship_date),
            d=COUNT(customers.WHERE(acctbal > 0))
        ).WHERE(
            c > 1000
        ).ORDER_BY(
            d.DESC()
        ),
        ```

        A valid string representation of this would be:

        ```
        ┌─── OrderBy[d.DESC()]
        ├─── Where[c > 1000]
        ├─── Calc[a=[ancestor.name], b=[name], c=[MAX($2.ship_date)], d=[COUNT($1)]]
        ├─┬─ AccessSubCollections
        │ ├─┬─ Where[acctbal > 0]
        │ │ └─── PluralSubCollection[customers]
        │ └─┬─ PluralSubCollection[lines]
        │   └─┬─ PluralSubCollection[supply_records]
        │     └─── PluralSubCollection[suppliers]
        └─┬─ PluralSubCollection[nations]
          ├─── Where[STARTSWITH(name, 'A')]
          └─── TableCollection[Regions]
        ```

        Returns:
            The tree-like string representation of `self`.
        """
