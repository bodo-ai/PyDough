"""
PyDough session configuration used for end to end processing
of PyDough execution or code generation. This session tracks
important information like:
- The active metadata graph.
- Any PyDough configuration for function behavior.
- Backend information (SQL dialect, Database connection, etc.)

In the future this session will also contain other information
such as any User Defined registration for additional backend
functionality that should not be merged to main repository.

The intended use of a session is that by default the PyDough project
will maintain an active session and will use this information to process
any user code. By default most people will just modify the active session
(via property access syntax or methods), but in some cases users can also
swap out the active session for a brand new one if they want to preserve
existing state.
"""

from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
    empty_connection,
    load_database_context,
)
from pydough.metadata import GraphMetadata, parse_json_metadata_from_file

from .pydough_configs import PyDoughConfigs


class PyDoughSession:
    """
    Container class used to define a PyDough session. This includes both
    a set of properties that can be accessed and modified directly, as
    well as helper methods to assist with some of the plumbing.
    """

    def __init__(self) -> None:
        self._metadata: GraphMetadata | None = None
        self._config: PyDoughConfigs = PyDoughConfigs()
        # By default we have a backend that cannot execute any SQL but can
        # still generate ANSI SQL, since this doesn't require any database
        # setup. We create a new DatabaseContext each time instead of a
        # singleton since a user may opt to provide their own connection
        # by just swapping the connection attribute.
        self._database: DatabaseContext = DatabaseContext(
            connection=empty_connection, dialect=DatabaseDialect.ANSI
        )

    @property
    def metadata(self) -> GraphMetadata | None:
        """
        Get the active metadata graph.

        Returns:
            GraphMetadata: The active metadata graph.
        """
        return self._metadata

    @metadata.setter
    def metadata(self, graph: GraphMetadata | None) -> None:
        """
        Set the active metadata graph.

        Args:
            graph (GraphMetadata | None): The metadata graph to set.
        """
        self._metadata = graph

    @property
    def config(self) -> PyDoughConfigs:
        """
        Get the active PyDough configuration.

        Returns:
            PyDoughConfigs: The active PyDough configuration.
        """
        return self._config

    @config.setter
    def config(self, config: PyDoughConfigs) -> None:
        """
        Set the active PyDough configuration.

        Args:
            config (PyDoughConfigs): The PyDough configuration to set.
        """
        self._config = config

    @property
    def database(self) -> DatabaseContext:
        """
        Get the active database context.

        Returns:
            DatabaseContext: The active database context.
        """
        return self._database

    @database.setter
    def database(self, context: DatabaseContext) -> None:
        """
        Set the active database context.

        Args:
            context (DatabaseContext): The database context to set.
        """
        self._database = context

    def connect_database(self, database_name: str, **kwargs) -> DatabaseContext:
        """
        Create a new DatabaseContext and register it in the session. This returns
        the corresponding context in case the user wants/needs to modify it.

        Args:
            database_name (str): The name of the database to connect to.
            **kwargs: Additional keyword arguments to pass to the connection.
                All arguments must be accepted using the supported connect API
                for the dialect. Most likely the database path will be required.

        Returns:
            DatabaseContext: The newly created database context.
        """
        context: DatabaseContext = load_database_context(database_name, **kwargs)
        self.database = context
        return context

    def load_metadata_graph(self, graph_path: str, graph_name: str) -> GraphMetadata:
        """
        Load a metadata graph from a file and register it in the session.
        This returns the corresponding graph for use. If there is already
        an existing graph it will be removed from the session, although
        the user is free to maintain a reference to it and set the session
        property directly later.

        Args:
            graph_path (str): The path to load the graph. At this time this must be on
                the user's local file system.
            graph_name (str): The name under which to load the graph from the file. This
                is to allow loading multiple graphs from the same json file.

        Returns:
            GraphMetadata: The loaded metadata graph.
        """
        graph: GraphMetadata = parse_json_metadata_from_file(graph_path, graph_name)
        self.metadata = graph
        return graph
