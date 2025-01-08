#!/bin/bash

# Script should be run from within the `tests` directory to set up the defog.ai
# database with the tables that are used by the various schemas. The e2e
# defog tests cannot be run unless this commmand has already been used to set
# up the sqlite database.

set -eo pipefail

rm -fv defog.db
sqlite3 defog.db < init_defog.sql
