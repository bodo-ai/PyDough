"""
Top-level init file for PyDough package.

Copyright (C) 2024 Bodo Inc. All rights reserved.
"""

__all__ = [
    "parse_json_metadata_from_file",
    "init_pydough_context",
    "active_session",
    "to_sql",
    "to_df",
    "explain",
    "explain_structure",
    "explain_term",
    "display_raw",
]

from .configs import PyDoughSession
from .evaluation import to_df, to_sql
from .exploration import explain, explain_structure, explain_term
from .metadata import parse_json_metadata_from_file
from .unqualified import display_raw, init_pydough_context

# Create a default session for the user to interact with.
# In most situations users will just use this session and
# modify the components, but strictly speaking they are allowed
# to create their own session and swap it out if they want to.
active_session: PyDoughSession = PyDoughSession()
