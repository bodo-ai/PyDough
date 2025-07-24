"""
Base definition of all PyDough QDAG collection types.
"""

__all__ = ["PyDoughCollectionQDAG"]


import re
from abc import abstractmethod
from collections.abc import Iterable
from functools import cache, cached_property
from typing import Union

import numpy as np

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.qdag.expressions.collation_expression import CollationExpression
from pydough.qdag.expressions.expression_qdag import PyDoughExpressionQDAG

from .collection_tree_form import CollectionTreeForm


class PyDoughCollectionQDAG(PyDoughQDAG):
    """
    The base class for QDAG nodes that represent a table collection accessed
    as a root.
    """

    def __repr__(self):
        return self.to_string()

    @property
    @abstractmethod
    def name(self) -> str:
        """
        The name of the collection.
        """

    @cache
    def get_ancestral_names(self) -> list[str]:
        """
        The names of all ancestors of the collection, starting from the top.
        """
        if self.ancestor_context is None:
            return []
        return self.ancestor_context.get_ancestral_names() + [
            self.ancestor_context.name
        ]

    @property
    def ancestor_context(self) -> Union["PyDoughCollectionQDAG", None]:
        """
        The ancestor context from which this collection is derived. Returns
        None if there is no ancestor context because the collection is the top
        of the hierarchy.
        """

    @property
    @abstractmethod
    def preceding_context(self) -> Union["PyDoughCollectionQDAG", None]:
        """
        The preceding context from which this collection is derived, e.g. an
        ORDER BY term before a CALCULATE. Returns None if there is no preceding
        context, e.g. because the collection is the start of a pipeline
        within a larger ancestor context.
        """

    @property
    @abstractmethod
    def calc_terms(self) -> set[str]:
        """
        The list of expressions that would be retrieved if the collection
        were to have its results evaluated. This is the set of names in the
        most-recent CALCULATE, potentially with extra expressions added since
        then.
        """

    @property
    @abstractmethod
    def all_terms(self) -> set[str]:
        """
        The set of expression/subcollection names accessible by the context.
        """

    @property
    @abstractmethod
    def ancestral_mapping(self) -> dict[str, int]:
        """
        A mapping of names created by the current context and its ancestors
        describing terms defined inside a CALCULATE clause that are available
        to the current context & descendants to back-reference via that name
        to the number of ancestors up required to find the back-referenced
        term.
        """

    @property
    @abstractmethod
    def inherited_downstreamed_terms(self) -> set[str]:
        """
        A set of names created by indirect ancestors of the current context
        that can be used to back-reference. The specific index of the
        back-reference is handled during the hybrid conversion process, when
        implicit back-references are flushed to populate the base of the tree
        input to a PARTITION node.
        """

    @abstractmethod
    def is_singular(self, context: "PyDoughCollectionQDAG") -> bool:
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

    def is_ancestor(self, collection: "PyDoughCollectionQDAG") -> bool:
        """
        Returns whether the current collection is an ancestor of the given
        collection.

        Args:
            `collection`: the collection that is being checked against.

        Returns:
            True if the current collection is an ancestor of `collection`,
            and False otherwise.
        """
        if collection.ancestor_context is self:
            return True
        if collection.ancestor_context is None:
            return False
        return self.is_ancestor(collection.ancestor_context)

    @cached_property
    def starting_predecessor(self) -> "PyDoughCollectionQDAG":
        """
        Returns the predecessor at the start of the current chain of preceding
        collections, or `self` if this is the start of that chain. The process
        also unwraps any ChildOperatorChildAccess terms.
        """
        from pydough.qdag.collections import ChildOperatorChildAccess

        predecessor: PyDoughCollectionQDAG | None = self.preceding_context
        result: PyDoughCollectionQDAG
        if predecessor is None:
            result = self
        else:
            result = predecessor.starting_predecessor
        while isinstance(result, ChildOperatorChildAccess):
            result = result.child_access.starting_predecessor
        return result

    def verify_singular_terms(self, exprs: Iterable[PyDoughExpressionQDAG]) -> None:
        """
        Verifies that a list of expressions is singular with regards to the
        current collection, e.g. they can used as CALCULATE terms.

        Args:
            `exprs`: the list of expression to be checked.

        Raises:
            `PyDoughQDAGException` if any element of `exprs` is not singular with
            regards to the current collection.
        """
        relative_context: PyDoughCollectionQDAG = self.starting_predecessor
        for expr in exprs:
            if not expr.is_singular(relative_context):
                raise PyDoughQDAGException(
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
            `PyDoughQDAGException` if `expr_name` is not a name of one of the
            expressions in `calc_terms`.
        """

    @abstractmethod
    def get_term(self, term_name: str) -> PyDoughQDAG:
        """
        Obtains an expression or collection accessible from the current context
        by name.

        Args:
            `term_name`: the name of the term that is being extracted.


        Returns:
            `PyDoughQDAGException` if `term_name` is not a name of one of the
            terms accessible in the context.
        """

    def get_expr(self, term_name: str) -> PyDoughExpressionQDAG:
        """
        Obtains an expression accessible from the current context by name.

        Args:
            `term_name`: the name of the term that is being extracted.


        Returns:
            `PyDoughQDAGException` if `term_name` is not a name of one of the
            terms accessible in the context, or is not an expression.
        """
        term = self.get_term(term_name)
        if not isinstance(term, PyDoughExpressionQDAG):
            raise PyDoughQDAGException(
                f"Property {term_name!r} of {self} is not an expression"
            )
        return term

    def get_collection(self, term_name: str) -> "PyDoughCollectionQDAG":
        """
        Obtains a collection accessible from the current context by name.

        Args:
            `term_name`: the name of the term that is being extracted.


        Returns:
            `PyDoughQDAGException` if `term_name` is not a name of one of the
            terms accessible in the context, or is not a collection.
        """
        term = self.get_term(term_name)
        if not isinstance(term, PyDoughCollectionQDAG):
            raise PyDoughQDAGException(
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
    def unique_terms(self) -> list[str]:
        """
        Returns the list of names of terms that cause records of the collection
        to be uniquely identifiable.
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
        Returns a PyDough collection QDAG converted to a simple string
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
        Returns a PyDough collection QDAG converted into a tree-like string,
        structured. For example, consider the following PyDough snippet:

        ```
        Regions.CALCULATE(
            region_name=name,
        ).WHERE(
            ENDSWITH(name, 's')
        ).nations.WHERE(
            name != 'USA'
        ).CALCULATE(
            a=region_name,
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
          ├─── Calculate[region_name=name]
          └─┬─ Where[ENDSWITH(name, 's')]
            ├─── SubCollection[nations]
            ├─── Where[name != 'USA']
            ├─┬─ Calculate[a=[region_name], b=[name], c=[MAX($2._expr1)], d=[COUNT($1)]]
            │ ├─┬─ AccessChild
            │ │ ├─ SubCollection[customers]
            │ │ └─── Where[acctbal > 0]
            │ └─┬─ AccessChild
            │   └─┬─ SubCollection[suppliers]
            │     ├─── Where[STARTSWITH(phone, '415')]
            │     └─┬─ SubCollection[supply_records]
            │       └─┬─ SubCollection[lines]
            │         └─── Calculate[_expr1=YEAR(ship_date)]
            ├─── Where[c > 1000]
            └─── OrderBy[d.DESC()]
        ```

        Returns:
            The tree-like string representation of `self`.
        """
        return "\n".join(self.to_tree_form(True).to_string_rows())

    def find_possible_name_matches(self, term_name: str) -> list[str]:
        """
        Finds and returns a list of candidate names that closely match the
        given name based on minimum edit distance.

        Args:
            `term_name`: The name to match against the list of candidates.

        Returns:
            A list of candidate names, based on the closest matches.
        """

        terms_distance_list: list[tuple[float, str]] = []

        for term in self.all_terms:
            # get the minimum edit distance
            me: float = self.min_edit_distance(term_name, term)
            terms_distance_list.append((me, term))

        if terms_distance_list == []:
            return []
        # sort the list by minimum edit distance break ties by name
        terms_distance_list.sort()

        closest_match = terms_distance_list[0]

        # List with all names that have a me <= closest_match + 2
        matches_within_2: list[str] = []
        # List with all names that have a me <= closest_match * 1.1
        matches_within_10_pct: list[str] = []
        # List with the top 3 closest matches (me) breaking ties by name
        matches_top_3: list[str] = [name for _, name in terms_distance_list[:3]]

        # filtering the result
        for me, name in terms_distance_list:
            # all names that have a me <= closest_match + 2
            if me <= closest_match[0] + 2:
                matches_within_2.append(name)

            # all names that have a me <= closest_match * 1.1
            if me <= closest_match[0] * 1.1:
                matches_within_10_pct.append(name)

        # returning the larger
        # using
        return max(
            [matches_within_2, matches_within_10_pct, matches_top_3],
            key=lambda x: (len(x), x),
        )

    @staticmethod
    def min_edit_distance(s: str, t: str) -> float:
        """
        Computes the minimum edit distance between two strings using the
        Levenshtein distance algorithm. Substituting a character for the same
        character with different capitalization is considered 10% of the edit
        cost of replacing it with any other character. For this implementation
        the iterative with a 2-row array is used to save memory.
        Link:
        https://en.wikipedia.org/wiki/Levenshtein_distance#Iterative_with_two_matrix_rows

        Args:
            `s`: The first string.
            `t`: The second string.

        Returns:
            The minimum edit distance between the two strings.
        """
        # Ensures str1 is the shorter string
        if len(s) > len(t):
            s, t = t, s
        m, n = len(s), len(t)

        # Use a 2 x (m + 1) array to represent an n x (m + 1) array since you only
        # need to consider the previous row to generate the next row, therefore the
        # same two rows can be recycled

        row, previousRow = 1, 0
        arr = np.zeros((2, m + 1), dtype=float)

        # MED(X, "") = len(X)
        arr[0, :] = np.arange(m + 1)

        for i in range(1, n + 1):
            # MED("", X) = len(X)
            arr[row, 0] = i

            # Loop over the rest of s to see if it matches with the corresponding
            # letter of t
            for j in range(1, m + 1):
                substitution_cost: float

                if s[j - 1] == t[i - 1]:
                    substitution_cost = 0.0
                elif s[j - 1].lower() == t[i - 1].lower():
                    substitution_cost = 0.1
                else:
                    substitution_cost = 1.0

                arr[row, j] = min(
                    arr[row, j - 1] + 1.0,
                    arr[previousRow, j] + 1.0,
                    arr[previousRow, j - 1] + substitution_cost,
                )

            row, previousRow = previousRow, row

        return arr[previousRow, m]  # Return the last computed row's last element

    def name_mismatch_error(self, term_name: str) -> str:
        """
        Raises a name mismatch error with suggestions if possible.
        Args:
            term_name (str): The name of the term that caused the error.
        """

        error_message: str = f"Unrecognized term of {self.to_string()}: {term_name!r}."
        suggestions: list[str] = self.find_possible_name_matches(term_name=term_name)

        # Check if there are any suggestions to add
        if len(suggestions) > 0:
            suggestions_str: str = ", ".join(suggestions)
            error_message += f" Did you mean: {suggestions_str}?"
            re.escape(error_message)

        return error_message
