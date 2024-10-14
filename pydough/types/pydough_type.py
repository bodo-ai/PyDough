"""
TODO: add file-level docstring
"""


class PyDoughType(object):
    """
    TODO: add class docstring
    """

    def __init__(self):
        raise NotImplementedError(
            f"PyDough type class {type(self).__name__} does not have an __init__ defined"
        )

    def __repr__(self):
        raise NotImplementedError(
            f"PyDough type class {type(self).__name__} does not have a __repr__ defined"
        )

    def as_json_string(self):
        """
        TODO: add function docstring
        """
        raise NotImplementedError(
            f"PyDough type class {type(self).__name__} does not have a as_json_string method"
        )

    def __eq__(self, other):
        return isinstance(other, PyDoughType) and repr(self) == repr(other)

    def __hash__(self):
        raise hash(repr(self))
