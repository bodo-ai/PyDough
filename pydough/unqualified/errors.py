"""
Error handling definitions used for the unqualified module.

Copyright (C) 2024 Bodo Inc. All rights reserved.
"""

__all__ = ["PyDoughUnqualifiedException"]


class PyDoughUnqualifiedException(Exception):
    """Exception raised when there is an error relating to the PyDough
    unqualified form, such as a Python object that cannot be coerced.
    """
