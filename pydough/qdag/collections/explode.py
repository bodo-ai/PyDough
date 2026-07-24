"""
Base definition of PyDough QDAG collection type for the explode operation.
"""

__all__ = ["Explode"]


import pydough
from pydough.errors import PyDoughQDAGException
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions import (
    BackReferenceExpression,
    CollationExpression,
    PyDoughExpressionQDAG,
    Reference,
)
from pydough.types import ArrayType, NumericType, PyDoughType, UnknownType
from pydough.utilities import ExplodeSpec

from .child_access import ChildAccess
from .collection_qdag import PyDoughCollectionQDAG


class Explode(ChildAccess):
    """
    The QDAG node implementation class representing an explode operation.
    """

    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
        data: PyDoughExpressionQDAG,
        name: str,
        explode_spec: ExplodeSpec,
    ):
        super().__init__(ancestor)
        if explode_spec.value_name in ancestor.all_terms:
            raise PyDoughQDAGException(
                f"Cannot use {explode_spec.value_name!r} as the `value_name` for EXPLODE because it is already a term in the ancestor context"
            )
        if (
            explode_spec.index_name is not None
            and explode_spec.index_name in ancestor.all_terms
        ):
            raise PyDoughQDAGException(
                f"Cannot use {explode_spec.index_name!r} as the `index_name` for EXPLODE because it is already a term in the ancestor context"
            )
        self._name: str = name
        self._data: PyDoughExpressionQDAG = data
        self._explode_spec: ExplodeSpec = explode_spec
        self._all_property_names: set[str] = set()
        # Build the current node's ancestral mapping by copying the ancestor's
        # mapping and incrementing each level by 1 to reflect
        # the added depth of this node.
        self._ancestral_mapping: dict[str, int] = {
            name: level + 1 for name, level in ancestor.ancestral_mapping.items()
        }
        self._all_property_names.update(self._ancestral_mapping)
        self._all_property_names.add(explode_spec.value_name)
        if explode_spec.index_name is not None:
            self._all_property_names.add(explode_spec.index_name)

    def clone_with_parent(self, new_parent: PyDoughCollectionQDAG) -> "Explode":
        return Explode(
            new_parent,
            self.data,
            self.name,
            self.explode_spec,
        )

    @property
    def data(self) -> PyDoughExpressionQDAG:
        """
        The data that will be exploded by the operation.
        """
        return self._data

    @property
    def name(self) -> str:
        """
        The name of the collection after being exploded. This is the name that
        will be used to reference the collection in subsequent operations, such
        as window functions.
        """
        return self._name

    @property
    def key(self) -> str:
        return f"{self.ancestor_context.key}.EXPLODE"

    @property
    def explode_spec(self) -> ExplodeSpec:
        """
        The dataclass payload containing the specifications for the explode
        operation.
        """
        return self._explode_spec

    @property
    def calc_terms(self) -> set[str]:
        if self.explode_spec.index_name is None:
            return {self.explode_spec.value_name}
        else:
            return {self.explode_spec.value_name, self.explode_spec.index_name}

    @property
    def all_terms(self) -> set[str]:
        return self._all_property_names

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self._ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        return self.ancestor_context.inherited_downstreamed_terms

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    @property
    def unique_terms(self) -> list[str]:
        # Note: must add ancestral unique terms in the hybrid step
        if self.explode_spec.is_distinct:
            return [self.explode_spec.value_name]
        else:
            assert self.explode_spec.index_name is not None
            return [self.explode_spec.index_name]

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        return False

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name == self.explode_spec.value_name:
            return 0
        elif expr_name == self.explode_spec.index_name:
            return 1
        else:
            raise PyDoughQDAGException(f"Unrecognized term of {self!r}: {expr_name!r}")

    def get_term(self, term_name: str) -> PyDoughQDAG:
        if term_name not in self.all_terms:
            if term_name in self.ancestor_context.all_terms:
                result: PyDoughQDAG = self.ancestor_context.get_term(term_name)
                if isinstance(result, PyDoughExpressionQDAG):
                    if isinstance(result, BackReferenceExpression):
                        return BackReferenceExpression(
                            self, term_name, result.back_levels + 1
                        )
                    return BackReferenceExpression(self, term_name, 1)
                else:
                    return result
            else:
                raise pydough.active_session.error_builder.term_not_found(
                    collection=self, term_name=term_name
                )

        # Special handling of terms down-streamed from an ancestor CALCULATE
        # clause.
        if term_name in self.ancestral_mapping:
            # Verify that the ancestor name is not also a name in the current
            # context.
            if term_name in self.calc_terms:
                raise pydough.active_session.error_builder.downstream_conflict(
                    collection=self, term_name=term_name
                )
            # Create a back-reference to the ancestor term.
            return BackReferenceExpression(
                self, term_name, self.ancestral_mapping[term_name]
            )

        if term_name in self.inherited_downstreamed_terms:
            context: PyDoughCollectionQDAG = self
            while term_name not in context.all_terms:
                if context is self:
                    context = self.ancestor_context
                else:
                    assert context.ancestor_context is not None
                    context = context.ancestor_context
            return Reference(
                context, term_name, context.get_expr(term_name).pydough_type
            )

        typ: PyDoughType
        if term_name == self.explode_spec.value_name:
            if isinstance(self._data.pydough_type, ArrayType):
                typ = self._data.pydough_type.elem_type
            else:
                typ = UnknownType()
        else:
            assert term_name == self.explode_spec.index_name
            typ = NumericType()

        return Reference(self, term_name, typ)

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

    @property
    def standalone_string(self) -> str:
        terms: list[str] = [
            f"EXPLODE[{self.data.to_string()}",
            f"name={self.name!r}",
            self.explode_spec.keyword_arg_string,
        ]
        return ", ".join(terms)

    @property
    def tree_item_string(self) -> str:
        base_str: str = self.standalone_string
        return f"EXPLODE[{base_str[8:-1]}]"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, Explode)
            and super().equals(other)
            and self.data == other.data
            and self.name == other.name
            and self.explode_spec == other.explode_spec
        )
