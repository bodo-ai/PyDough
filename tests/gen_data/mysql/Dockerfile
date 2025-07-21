# Use the official MySQL base image
FROM mysql:8.0

ENV MYSQL_DATABASE=tpch
ENV MYSQL_ROOT_PASSWORD=lqsymdocker10

# Copies the tpch database schema
COPY 01_schema.sql /docker-entrypoint-initdb.d/

# Copies the .tbl files (which have the data)
COPY *.tbl /var/lib/mysql-files/

# Copies the sql that loads the file in the created schema
COPY 02_data.sql /docker-entrypoint-initdb.d/

