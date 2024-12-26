#!/bin/bash

set -eo pipefail

# Link the TPC-H database. Use a symlink to
# avoid copying the data at launch.
ln -s /usr/local/share/tpch.db ./tpch.db

# Build the PyDough project
python -m pip install .

# Build the Juypter Extension
cd pydough_jupyter_extensions && python -m pip install .
