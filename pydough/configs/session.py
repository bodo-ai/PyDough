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
