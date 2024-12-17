FROM mcr.microsoft.com/devcontainers/universal:2

COPY /demos/setup_tpch.sh setup_tpch.sh

# Download the TPC-H database.
RUN ./setup_tpch.sh /usr/local/share/tpch.db
