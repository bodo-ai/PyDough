import ast

from IPython.core.magic import (
    Magics,
    cell_magic,
    magics_class,
    needs_local_scope,
)
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

import pydough
from pydough.metadata import GraphMetadata
from pydough.unqualified import transform_code


@magics_class
class PyDoughMagic(Magics):
    """
    Class that defines the magic command for running a Jupyter cell as a PyDough
    command.
    """

    def __init__(self, shell):
        Magics.__init__(self, shell=shell)
        self.shell.configurables.append(self)

    @needs_local_scope
    @magic_arguments()
    @argument(
        "--debug",
        action="store_true",
        help="Prints debug information about the AST transformation.",
    )
    @cell_magic
    def pydough(self, line="", cell="", local_ns=None):
        if local_ns is None:
            local_ns = {}
        cell = self.shell.var_expand(cell)
        # Parse the line command. This is primarily to allow debug information
        # to be printed.
        args = parse_argstring(self.pydough, line)
        graph: GraphMetadata | None = pydough.active_session.metadata
        if graph is None:
            raise Exception(
                "No active graph set in PyDough session. Please set a graph using pydough.active_session.load_metadata_graph(...)"
            )

        graph_dict: dict[str, GraphMetadata] = {
            "pydough.active_session.metadata": graph
        }
        # TODO: Find env
        new_tree: ast.AST = transform_code(cell, graph_dict, set(local_ns.keys()))
        if args.debug:
            print(ast.unparse(new_tree))
        assert isinstance(new_tree, ast.Module)
        new_cell: str = ast.unparse(new_tree)
        self.shell.run_cell(new_cell)
