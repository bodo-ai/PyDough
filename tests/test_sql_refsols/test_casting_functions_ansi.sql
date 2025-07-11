SELECT
  TIME_TO_STR(o_orderdate, '%Y-%m-%d') AS cast_to_string,
  CAST(o_totalprice AS BIGINT) AS cast_to_integer,
  CAST(o_shippriority AS DOUBLE) AS cast_to_float
FROM tpch.orders
