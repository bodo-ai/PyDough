from IPython.core.magic import (
    Magics,
    cell_magic,
    magics_class,
)


@magics_class
class PyDoughMagic(Magics):
    """
    Class that defines the magic command for running a Jupyter cell as a PyDough
    command.
    """

    def __init__(self, shell):
        print("REACHED INIT")
        Magics.__init__(self, shell=shell)
        self.shell.configurables.append(self)
        self._pydough_buffer = []

    @cell_magic
    def append_pydough(self, line="", cell="", local_ns={}):
        print("REACHED ME")
