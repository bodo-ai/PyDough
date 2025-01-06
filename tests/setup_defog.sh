#!/bin/bash

set -eo pipefail

rm -fv defog.db
sqlite3 defog.db < init_defog.sql
