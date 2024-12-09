from IPython.core.magic import (
    Magics,
    cell_magic,
    magics_class,
    needs_local_scope,
)
from IPython.core.magic_arguments import magic_arguments


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
    @magic_arguments()
    def append_pydough(self, line="", cell="", local_ns={}):
        pass
