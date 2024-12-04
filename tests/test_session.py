"""
Tests for the session module. This doesn't directly test the
active session but instead unit tests the core functionality
of any session.

For each of these tests we create a new session so that we can
manipulate the session without affecting other tests.
"""

from pydough.configs import PyDoughConfigs, PyDoughSession
from pydough.database_connectors import DatabaseDialect, empty_connection


def test_defaults() -> None:
    """
    Tests that a sessions defaults are set correctly.
    """
    session: PyDoughSession = PyDoughSession()
    assert session.metadata is None
    assert session.config is not None
    default_config: PyDoughConfigs = PyDoughConfigs()
    # TODO: Add an API to iterate and check all of the properties
    # match the defaults.
    assert session.config.sum_default_zero is default_config.sum_default_zero
    assert session.config.avg_default_zero is default_config.avg_default_zero
    assert session.database is not None
    assert session.database.connection is empty_connection
    assert session.database.dialect is DatabaseDialect.ANSI
