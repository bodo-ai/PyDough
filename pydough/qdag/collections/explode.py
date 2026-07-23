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
        value_name: str,
        index_name: str | None,
        version: str,
        delimiter: str | None,
        filtering: bool,
        is_distinct: bool,
    ):
        super().__init__(ancestor)
        if value_name in ancestor.all_terms:
            raise PyDoughQDAGException(
                f"Cannot use {value_name!r} as the `value_name` for EXPLODE because it is already a term in the ancestor context"
            )
        if index_name is not None and index_name in ancestor.all_terms:
            raise PyDoughQDAGException(
                f"Cannot use {index_name!r} as the `index_name` for EXPLODE because it is already a term in the ancestor context"
            )
        self._name: str = name
        self._data: PyDoughExpressionQDAG = data
        self._value_name: str = value_name
        self._index_name: str | None = index_name
        self._version: str = version
        self._delimiter: str | None = delimiter
        self._filtering: bool = filtering
        self._is_distinct: bool = is_distinct
        self._all_property_names: set[str] = set()
        # Build the current node's ancestral mapping by copying the ancestor's
        # mapping and incrementing each level by 1 to reflect
        # the added depth of this node.
        self._ancestral_mapping: dict[str, int] = {
            name: level + 1 for name, level in ancestor.ancestral_mapping.items()
        }
        self._all_property_names.update(self._ancestral_mapping)
        self._all_property_names.add(self._value_name)
        if self._index_name is not None:
            self._all_property_names.add(self._index_name)

    def clone_with_parent(self, new_parent: PyDoughCollectionQDAG) -> "Explode":
        return Explode(
            new_parent,
            self._data,
            self._name,
            self._value_name,
            self._index_name,
            self._version,
            self._delimiter,
            self._filtering,
            self._is_distinct,
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
    def value_name(self) -> str:
        """
        The name of the property that will hold the exploded values.
        """
        return self._value_name

    @property
    def index_name(self) -> str | None:
        """
        The name of the property that will hold the exploded indices, or None
        if no index property is being created.
        """
        return self._index_name

    @property
    def version(self) -> str:
        """
        The version of the explode operation, either "array" or "string".
        """
        return self._version

    @property
    def delimiter(self) -> str | None:
        """
        The delimiter to use when exploding a string column, or None if the
        version is "array".
        """
        return self._delimiter

    @property
    def filtering(self) -> bool:
        """
        Whether the explode operation can result in not every row from the
        original being included in the output, i.e. if some of the rows from
        `data` are empty arrays or empty strings.
        """
        return self._filtering

    @property
    def is_distinct(self) -> bool:
        """
        Whether each exploded value will be unique within the output with
        regards to the original row from which it was exploded.
        """
        return self._is_distinct

    @property
    def calc_terms(self) -> set[str]:
        if self._index_name is None:
            return {self._value_name}
        else:
            return {self._value_name, self._index_name}

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
        if self.is_distinct:
            return [self.value_name]
        else:
            assert self.index_name is not None
            return [self.index_name]  # Note: must add ancestral unique terms

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        return False

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name == self._value_name:
            return 0
        elif expr_name == self._index_name:
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
        if term_name == self._value_name:
            if isinstance(self._data.pydough_type, ArrayType):
                typ = self._data.pydough_type.elem_type
            else:
                typ = UnknownType()
        else:
            assert term_name == self._index_name
            typ = NumericType()

        return Reference(self, term_name, typ)

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

    @property
    def standalone_string(self) -> str:
        terms: list[str] = [
            f"EXPLODE[{self._data.to_string()}",
            f"name={self._name!r}",
            f"value_name={self._value_name!r}",
        ]
        if self._index_name is not None:
            terms.append(f"index_name={self._index_name!r}")
        terms.append(f"version={self._version!r}")
        if self._version == "string":
            terms.append(f"delimiter={self._delimiter!r}")
        terms.append(f"filtering={self._filtering}")
        terms.append(f"is_distinct={self._is_distinct})")
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
            and self.value_name == other.value_name
            and self.index_name == other.index_name
            and self.version == other.version
            and self.delimiter == other.delimiter
            and self.filtering == other.filtering
            and self.is_distinct == other.is_distinct
        )
