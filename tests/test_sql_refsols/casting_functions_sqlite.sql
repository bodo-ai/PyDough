SELECT
  STRFTIME('%Y-%m-%d', o_orderdate) AS cast_to_string,
  CAST(o_totalprice AS INTEGER) AS cast_to_integer,
  CAST(o_shippriority AS REAL) AS cast_to_float
FROM tpch.orders
