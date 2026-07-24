SELECT
  DATE_FORMAT(o_orderdate, 'yyyy-MM-dd') AS cast_to_string,
  CAST(o_totalprice AS STRING) AS cast_to_string2,
  CAST(CAST(o_totalprice AS DOUBLE) AS BIGINT) AS cast_to_integer,
  CAST(o_shippriority AS DOUBLE) AS cast_to_float
FROM tpch.orders
