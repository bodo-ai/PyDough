#!/bin/bash

set -eo pipefail

# Download the TPC-H database
./demos/setup_tpch.sh ./demos/tpch.db

# Build the PyDough project
python -m pip install .

# Build the Juypter Extension
cd pydough_jupyter_extensions && python -m pip install .

# Open the readme file
# TODO: FIXME. Code is not installed.
# code -r demos/README.md --open-to-side
