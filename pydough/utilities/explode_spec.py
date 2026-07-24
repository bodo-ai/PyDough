"""
Contains the definition of the ExplodeSpec dataclass,
"""

from dataclasses import dataclass


@dataclass
class ExplodeSpec:
    """
    Dataclass storing information about an EXPLODE operation being performed.
    """

    value_name: str
    index_name: str | None
    version: str
    delimiter: str | None
    filtering: bool
    is_distinct: bool

    @property
    def arg_list_string(self):
        args: list[str] = []
        args.append(self.value_name)
        if self.index_name is not None:
            args.append(self.index_name)
        args.append(self.version)
        if self.delimiter is not None:
            args.append(self.delimiter)
        args.append(repr(self.filtering))
        args.append(repr(self.is_distinct))
        return ", ".join(args)

    @property
    def keyword_arg_string(self):
        kwargs: list[str] = []
        kwargs.append(f"value_name={self.value_name!r}")
        if self.index_name is not None:
            kwargs.append(f"index_name={self.index_name!r}")
        kwargs.append(f"version={self.version!r}")
        if self.delimiter is not None:
            kwargs.append(f"delimiter={self.delimiter!r}")
        kwargs.append(f"filtering={self.filtering!r}")
        kwargs.append(f"is_distinct={self.is_distinct!r}")
        return ", ".join(kwargs)
