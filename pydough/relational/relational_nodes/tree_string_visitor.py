"""
Definition of a relational tree visitor used to convert relational trees into
a multiline string of their full tree-like representation.
"""

__all__ = ["RelationalVisitor"]


from .relational_visitor import RelationalVisitor


class TreeStringVisitor(RelationalVisitor):
    """
    RelationalVisitor implementation that converts relational nodes into a tree
    string.
    """

    def __init__(self):
        self.lines: list[str] = []
        self.depth = 0

    def visit_node(self, node) -> None:
        """
        Generic visitor implementation used by visit_xxx methods.
        """
        self.lines.append(f"{' ' * self.depth}{node.to_string(compact=True)}")
        self.depth += 1
        self.visit_inputs(node)
        self.depth -= 1

    def make_tree_string(self) -> str:
        """
        Method called after the visitor is run on the entire relational tree to
        convert the entire tree to a single tree-like string.
        """
        return "\n".join(self.lines)

    def reset(self) -> None:
        self.lines.clear()
        self.depth = 0

    def visit_scan(self, scan) -> None:
        self.visit_node(scan)

    def visit_join(self, join) -> None:
        self.visit_node(join)

    def visit_project(self, project) -> None:
        self.visit_node(project)

    def visit_filter(self, filter) -> None:
        self.visit_node(filter)

    def visit_aggregate(self, aggregate) -> None:
        self.visit_node(aggregate)

    def visit_limit(self, limit) -> None:
        self.visit_node(limit)

    def visit_empty_singleton(self, empty_singleton) -> None:
        self.visit_node(empty_singleton)

    def visit_root(self, root) -> None:
        self.visit_node(root)
