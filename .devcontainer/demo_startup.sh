#!/bin/bash

set -eo pipefail

# Download the TPC-H database
cp /usr/local/share/tpch.db demos/tpch.db

# Build the PyDough project
python -m pip install .

# Build the Juypter Extension
cd pydough_jupyter_extensions && python -m pip install .
