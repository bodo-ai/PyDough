#!/bin/bash

set -eo pipefail

ls

rm -fv defog.db
sqlite3 defog.db < init_defog.sql
