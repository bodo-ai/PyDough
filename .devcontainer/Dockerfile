FROM mcr.microsoft.com/devcontainers/universal:2

COPY /demos/setup_tpch.sh setup_tpch.sh

# Install uv for command line editing.
RUN wget -qO- https://astral.sh/uv/install.sh | sh

# Download the TPC-H database.
RUN ./setup_tpch.sh /usr/local/share/tpch.db
