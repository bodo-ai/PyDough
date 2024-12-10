from IPython.core.magic import (
    Magics,
    cell_magic,
    magics_class,
    needs_local_scope,
)

import pydough
from pydough.metadata import GraphMetadata
from pydough.unqualified import transform_cell


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
    @cell_magic
    def pydough(self, line="", cell="", local_ns=None):
        if local_ns is None:
            local_ns = {}
        cell = self.shell.var_expand(cell)
        graph: GraphMetadata | None = pydough.active_session.metadata
        if graph is None:
            raise Exception(
                "No active graph set in PyDough session."
                " Please set a graph using"
                " pydough.active_session.load_metadata_graph(...)"
            )
        # TODO: Consider a safer way to ensure pydough.active_session.metadata
        # exits
        new_cell: str = transform_cell(
            "pydough.active_session.metadata", cell, set(local_ns.keys())
        )
        self.shell.run_cell(new_cell)
