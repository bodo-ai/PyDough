"""
TODO: add file-level docstring
"""

__all__ = ["CollectionTreeForm"]


from typing import Union, MutableSequence


class CollectionTreeForm(object):
    """
    A class used for displaying PyDough collections in tree form.
    """

    def __init__(
        self,
        item_str: str,
        depth: int,
        has_predecessor: bool = False,
        has_successor: bool = False,
        has_children: bool = False,
        predecessor: Union["CollectionTreeForm", None] = None,
        nested_trees: MutableSequence["CollectionTreeForm"] | None = None,
    ):
        self.item_str: str = item_str
        self.depth: int = depth
        self.has_predecessor = has_predecessor or (predecessor is not None)
        self.has_successor: bool = has_successor
        self.has_children: bool = has_children
        self.predecessor: Union["CollectionTreeForm", None] = predecessor
        self.nested_trees: MutableSequence["CollectionTreeForm"] = (
            [] if nested_trees is None else nested_trees
        )

    ROOT_PREDECESSOR: str = "┌───"
    ROOT_PARENT: str = "──┬─"
    ROOT_PARENT_PREDECESSOR: str = "┌─┬─"

    SUCCESSOR: str = "├───"
    SUCCESSOR_PARENT: str = "├─┬─"
    BASE: str = "└───"
    BASE_PARENT: str = "└─┬─"

    PREDECESSOR_SPACER: str = "  "
    CHILD_SPACER: str = "│ "

    def to_string_rows(self) -> MutableSequence[str]:
        """
        Converts the tree form into the string representation in the format
        specified by `PyDoughCollectionAST.to_string`, but as a list where each
        element is one line of the string.

        Returns:
            The tree-like string representation of `self`.
        """
        answer: MutableSequence[str]
        prefix: str = self.PREDECESSOR_SPACER * self.depth
        if not self.has_predecessor:
            match (self.has_children, self.has_successor):
                case (False, False):
                    answer = [self.item_str]
                case (False, True):
                    answer = [f"{self.ROOT_PREDECESSOR} {self.item_str}"]
                case (True, False):
                    answer = [f"{self.ROOT_PARENT} {self.item_str}"]
                case (True, True):
                    answer = [f"{self.ROOT_PARENT_PREDECESSOR} {self.item_str}"]
                case _:
                    raise Exception("Malformed collection tree form")
        else:
            answer = (
                [] if self.predecessor is None else self.predecessor.to_string_rows()
            )
            match (self.has_children, self.has_successor):
                case (False, False):
                    answer.append(f"{prefix}{self.BASE} {self.item_str}")
                case (False, True):
                    answer.append(f"{prefix}{self.SUCCESSOR} {self.item_str}")
                case (True, False):
                    answer.append(f"{prefix}{self.BASE_PARENT} {self.item_str}")
                case (True, True):
                    answer.append(f"{prefix}{self.SUCCESSOR_PARENT} {self.item_str}")
                case _:
                    raise Exception("Malformed collection tree form")
        for idx, child in enumerate(self.nested_trees):
            new_prefix: str = f"{prefix}{self.PREDECESSOR_SPACER}"
            is_last_child: bool = idx == len(self.nested_trees) - 1
            for line in child.to_string_rows():
                if line[0] == " " and not is_last_child:
                    line = f"{new_prefix}{self.CHILD_SPACER}{line[2:]}"
                else:
                    line = f"{new_prefix}{line}"
                answer.append(line)
        return answer
