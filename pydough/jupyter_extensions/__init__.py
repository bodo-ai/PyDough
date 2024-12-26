from .pydough_magic import PyDoughMagic


def load_ipython_extension(ipython):
    """
    Register the magics with IPython
    """
    ipython.register_magics(PyDoughMagic)
