"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughUnqualifiedException"]


class PyDoughUnqualifiedException(Exception):
    """Exception raised when there is an error relating to the PyDough
    unqualified form, such as a Python object that cannot be coerced.
    """
