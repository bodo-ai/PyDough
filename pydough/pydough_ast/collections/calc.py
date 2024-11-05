"""
TODO: add file-level docstring
"""

__all__ = ["Calc"]


from collections.abc import MutableMapping, MutableSequence

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


class Calc(PyDoughCollectionAST):
    """
    The AST node implementation class representing a CALC expression.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ):
        self._predecessor: PyDoughCollectionAST = predecessor
        self._children: MutableSequence[PyDoughCollectionAST] = children
        # Not initialized until with_terms is called
        self._calc_term_indices: MutableMapping[str, int] | None = None
        self._all_terms: MutableMapping[str, PyDoughAST] = {}

    def with_terms(
        self, terms: MutableSequence[tuple[str, PyDoughExpressionAST]]
    ) -> "Calc":
        """
        Specifies the terms that are calculated inside of a CALC node,
        returning the mutated CALC node afterwards. This is called after the
        CALC node is created so that the terms can be expressions that
        reference child nodes of the CALC. However, this must be called
        on the CALC node before any properties are accessed by `calc_terms`,
        `all_terms`, `to_string`, etc.

        Args:
            `terms`: the list of terms calculated in the CALC node as a list of
            tuples in the form `(name, expression)`. Each `expression` can
            contain `ChildReference` instances that refer to an property of one
            of the children of the CALC node.

        Returns:
            The mutated CALC node (which has also been modified in-place).

        Raises:
            `PyDoughASTException` if the terms have already been added to the
            CALC node.
        """
        if self._calc_term_indices is not None:
            raise PyDoughASTException(
                "Cannot call `with_terms` on a CALC node more than once"
            )
        self._calc_term_indices = {name: idx for idx, (name, _) in enumerate(terms)}
        # Include terms from the predecessor, with the terms from this CALC
        # added in (overwriting any preceding properties with the same name)
        self._all_terms = {}
        preceding_context = self.preceding_context
        assert preceding_context is not None
        for name in preceding_context.all_terms:
            self._all_terms[name] = preceding_context.get_term(name)
        for name, property in terms:
            self._all_terms[name] = property
        return self

    @property
    def children(self) -> MutableSequence[PyDoughCollectionAST]:
        """
        The child collections accessible from the CALC used to derive
        expressions in terms of a subcollection.
        """
        return self._children

    @property
    def calc_term_indices(
        self,
    ) -> MutableMapping[str, int]:
        """
        MutableMapping of each named expression of the CALC to the index of the
        ordinal position of the property when included in a CALC.
        """
        if self._calc_term_indices is None:
            raise PyDoughASTException(
                "Cannot access `calc_term_indices` of a Calc node before adding calc terms with `with_terms`"
            )
        return self._calc_term_indices

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return self._predecessor.ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionAST:
        return self._predecessor

    @property
    def calc_terms(self) -> set[str]:
        return set(self.calc_term_indices)

    @property
    def all_terms(self) -> set[str]:
        if self._calc_term_indices is None:
            raise PyDoughASTException(
                "Cannot access `all_terms` of a Calc node before adding calc terms with `with_terms`"
            )
        return set(self._all_terms)

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.calc_term_indices:
            raise PyDoughASTException(f"Unrecognized CALC term: {expr_name!r}")
        return self.calc_term_indices[expr_name]

    def get_term(self, term_name: str) -> PyDoughAST:
        if term_name not in self.all_terms:
            raise PyDoughASTException(f"Unrecognized term: {term_name!r}")
        return self._all_terms[term_name]

    def to_string(self) -> str:
        kwarg_strings: MutableSequence[str] = []
        for name in self.calc_term_indices:
            expr: PyDoughExpressionAST = self.get_expr(name)
            kwarg_strings.append(f"{name}={expr.to_string()}")
        return f"{self.preceding_context.to_string()}({', '.join(kwarg_strings)})"

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.preceding_context.to_tree_form()
        predecessor.has_successor = True
        kwarg_strings: MutableSequence[str] = []
        for name in self.calc_term_indices:
            expr: PyDoughExpressionAST = self.get_expr(name)
            kwarg_strings.append(f"{name}={expr.to_string(tree_form=True)}")
        tree_form: CollectionTreeForm = CollectionTreeForm(
            f"Calc[{', '.join(kwarg_strings)}]",
            predecessor.depth,
            predecessor=predecessor,
        )
        for child in self.children:
            child_tree: CollectionTreeForm = child.to_tree_form()
            tree_form.has_children = True
            tree_form.nested_trees.append(child_tree)
        return tree_form

    def equals(self, other: object) -> bool:
        if self._all_terms is None:
            raise PyDoughCollectionAST(
                "Cannot invoke `equals` before calling `with_terms`"
            )
        return (
            isinstance(other, Calc)
            and self.preceding_context == other.preceding_context
            and self._calc_term_indices == other._calc_term_indices
        )
