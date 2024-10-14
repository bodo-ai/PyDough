class PyDoughType(object):
    """
    TODO: add class docstring
    """

    def __init__(self):
        raise NotImplementedError

    def __repr__(self):
        raise NotImplementedError

    def as_json_string(self):
        raise NotImplementedError

    def __eq__(self, other):
        return isinstance(other, PyDoughType) and repr(self) == repr(other)

    def __hash__(self):
        raise hash(repr(self))
