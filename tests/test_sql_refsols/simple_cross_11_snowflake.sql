WITH _s0 AS (
  SELECT
    o_orderdate
  FROM tpch.orders
), _s1 AS (
  SELECT
    MIN(o_orderdate) AS min_o_orderdate
  FROM _s0
)
SELECT
  COUNT(*) AS n
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.o_orderdate = _s1.min_o_orderdate
