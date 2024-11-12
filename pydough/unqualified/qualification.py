"""
TODO: add file-level docstring
"""

__all__ = ["qualify_node"]


from pydough.pydough_ast import PyDoughAST

from .unqualified_node import UnqualifiedNode


def qualify_node(unqualified: UnqualifiedNode) -> PyDoughAST:
    """
    Transforms an `UnqualifiedNode` into a qualified node.

    Args:
        `unqualified`: the UnqualifiedNode instance to be transformed.

    Returns:
        The PyDough AST object for the qualified node.

    Raises:
        `PyDoughUnqualifiedException` or `PyDoughASTException` if something
        goes wrong during the qualification process, e.g. a term cannot be
        qualified or is not recognized.
    """
    raise NotImplementedError
