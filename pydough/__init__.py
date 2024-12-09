"""
TODO: add module-level docstring
"""

__all__ = [
    "parse_json_metadata_from_file",
    "init_pydough_context",
    "explain_meta",
    "active_session",
]

from .configs import PyDoughSession
from .metadata import explain_meta, parse_json_metadata_from_file
from .unqualified import init_pydough_context

# Create a default session for the user to interact with.
# In most situations users will just use this session and
# modify the components, but strictly speaking they are allowed
# to create their own session and swap it out if they want to.
active_session: PyDoughSession = PyDoughSession()
