# TPCH Demo

This directory contains a set of scripts to demonstrate PyDough syntax and
functionality using the TPC-H queries and associated schema. This is not
intended as a benchmarking tool, but rather as a demonstration of how PyDough
can be used to answer analytics questions.

This demo uses a SQLite database to store the underlying data with scale factor 1.0.
We did not generate this data and instead obtained it from (TPCH-sqlite)[https://github.com/lovasoa/TPCH-sqlite],
which as the name suggests simplifies the process of loading the TPC-H data into SQLite.

## Configuring SQLite
To setup your Sqlite database we provide the a script, `setup_tpch.sh`. This script will download the TPC-H data and create your database file. You can execute this script in your shell of choice with the following command:

```bash
./setup_tpch.sh <database_file_path>
```

Note: This script will not combine with an existing database file and will fail if the file already exists at the provided path.

## Demo Dependencies
In addition to the set of PyDough dependencies, the TPCH demo requires the following
Python modules to be installed:

- sqlite3

## Demo Scripts
TODO
