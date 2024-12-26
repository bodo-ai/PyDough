FROM mcr.microsoft.com/devcontainers/universal:2

COPY /demos/setup_tpch.sh setup_tpch.sh

# Install uv for command line editing.
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

ADD https://astral.sh/uv/install.sh /uv-installer.sh

ENV HOME=/root

RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

# Download the TPC-H database.
RUN ./setup_tpch.sh /usr/local/share/tpch.db
