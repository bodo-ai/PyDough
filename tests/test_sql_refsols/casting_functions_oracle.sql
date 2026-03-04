SELECT
  TO_CHAR(o_orderdate, 'YYYY-MM-DD') AS cast_to_string,
  CAST(o_totalprice AS VARCHAR2(4000)) AS cast_to_string2,
  TRUNC(CAST(o_totalprice AS DOUBLE PRECISION), '0') AS cast_to_integer,
  CAST(o_shippriority AS DOUBLE PRECISION) AS cast_to_float
FROM TPCH.ORDERS
