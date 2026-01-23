WITH _s0 AS (
  SELECT
    c_custkey,
    c_name
  FROM tpch.customer
  ORDER BY
    2 NULLS FIRST
  LIMIT 5
), _t AS (
  SELECT
    o_custkey,
    o_orderdate,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) AS _w
  FROM tpch.orders
), _s1 AS (
  SELECT
    o_custkey,
    o_orderdate
  FROM _t
  WHERE
    _w = 1
)
SELECT
  _s0.c_name AS name
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s0.c_custkey = _s1.o_custkey
ORDER BY
  _s1.o_orderdate
