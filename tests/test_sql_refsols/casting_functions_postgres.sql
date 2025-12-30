SELECT
  TO_CHAR(o_orderdate, 'YYYY-MM-DD') AS cast_to_string,
  CAST(o_totalprice AS TEXT) AS cast_to_string2,
  TRUNC(CAST(o_totalprice AS DECIMAL)) AS cast_to_integer,
  CAST(o_shippriority AS DOUBLE PRECISION) AS cast_to_float
FROM tpch.orders
