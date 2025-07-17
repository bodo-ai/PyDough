SELECT
  DATE_FORMAT(o_orderdate, '%Y-%m-%d') AS cast_to_string,
  CAST(o_totalprice AS CHAR) AS cast_to_string2,
  CAST(o_totalprice AS SIGNED) AS cast_to_integer,
  CAST(o_shippriority AS DOUBLE) AS cast_to_float
FROM tpch.ORDERS
