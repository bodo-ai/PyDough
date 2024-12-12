"""
TODO: add module-level docstring
"""

__all__ = [
    "parse_json_metadata_from_file",
    "init_pydough_context",
    "active_session",
    "to_sql",
    "to_df",
    "display_raw",
]

from .configs import PyDoughSession
from .evaluation import to_df, to_sql
from .metadata import parse_json_metadata_from_file
from .unqualified import display_raw, init_pydough_context

# Create a default session for the user to interact with.
# In most situations users will just use this session and
# modify the components, but strictly speaking they are allowed
# to create their own session and swap it out if they want to.
active_session: PyDoughSession = PyDoughSession()
