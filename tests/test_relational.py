"""
TODO: add file-level docstring
"""

from pydough.pydough_ast.expressions.literal import Literal
from pydough.relational import Column
from pydough.types import Int64Type


def test_column_equal():
    """
    Test that the column definition properly implements equality
    based on its elements.
    """
    column1 = Column("a", Literal(1, Int64Type()))
    column2 = Column("a", Literal(1, Int64Type()))
    column3 = Column("b", Literal(1, Int64Type()))
    column4 = Column("a", Literal(2, Int64Type()))
    assert column1 == column2
    assert column1 != column3
    assert column1 != column4
