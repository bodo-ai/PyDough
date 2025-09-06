#!/bin/bash

# Script should be run from within the `tests` directory to set up the world
# development indicators database for use in tests The e2e synthea tests cannot
# be run unless this commmand has already been used to set up the sqlite 
# database.

set -eo pipefail

rm -fv world_development_indicators.db
sqlite3 world_development_indicators.db < init_world_indicators_sqlite.sql
