SELECT
  TO_CHAR(CAST(o_orderdate AS TIMESTAMP), 'yyyy-mm-DD') AS cast_to_string,
  CAST(o_totalprice AS TEXT) AS cast_to_string2,
  CAST(o_totalprice AS BIGINT) AS cast_to_integer,
  CAST(o_shippriority AS DOUBLE) AS cast_to_float
FROM tpch.orders
