"""
Top-level init file for PyDough package.
"""

__all__ = [
    "active_session",
    "display_raw",
    "explain",
    "explain_structure",
    "explain_term",
    "get_logger",
    "init_pydough_context",
    "parse_json_metadata_from_file",
    "to_df",
    "to_sql",
]

from .configs import PyDoughSession
from .evaluation import to_df, to_sql
from .exploration import explain, explain_structure, explain_term
from .logger import get_logger
from .metadata import parse_json_metadata_from_file
from .unqualified import display_raw, init_pydough_context

# Create a default session for the user to interact with.
# In most situations users will just use this session and
# modify the components, but strictly speaking they are allowed
# to create their own session and swap it out if they want to.
active_session: PyDoughSession = PyDoughSession()
