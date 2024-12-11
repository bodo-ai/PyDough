"""
TODO: add module-level docstring
"""

__all__ = [
    "parse_json_metadata_from_file",
    "init_pydough_context",
    "explain",
    "active_session",
    "explain_structure",
    "explain_term",
]

from .configs import PyDoughSession
from .exploration import explain, explain_structure, explain_term
from .metadata import parse_json_metadata_from_file
from .unqualified import init_pydough_context

# Create a default session for the user to interact with.
# In most situations users will just use this session and
# modify the components, but strictly speaking they are allowed
# to create their own session and swap it out if they want to.
active_session: PyDoughSession = PyDoughSession()
