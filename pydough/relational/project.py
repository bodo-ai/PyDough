"""
This file contains the relational implementation for a "project". This is our
relational representation for a "calc" that involves any compute steps and can include
adding or removing columns (as well as technically reordering). In general, we seek to
avoid introducing extra nodes just to reorder or prune columns, so ideally their use
should be sparse.
"""

from .abstract import Relational


class Project(Relational):
    """
    The Project node in the relational tree. This node represents a "calc" in
    relational algebra, which should involve some "compute" functions and may
    involve adding, removing, or reordering columns.
    """
