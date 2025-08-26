#!/bin/bash

# Script should be run from within the `tests` directory to set up the synthea
# database with the tables that are used by the various schemas. The e2e
# synthea tests cannot be run unless this commmand has already been used to set
# up the sqlite database.

set -eo pipefail

rm -fv synthea.db
sqlite3 synthea.db < init_synthea_sqlite.sql
