WITH _t AS (
  SELECT
    o_custkey,
    o_totalprice,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate DESC, o_orderkey) AS _w
  FROM tpch.orders
), _s1 AS (
  SELECT
    o_custkey,
    SUM(o_totalprice) AS sum_o_totalprice
  FROM _t
  WHERE
    _w <= 5
  GROUP BY
    1
)
SELECT
  customer.c_name AS name,
  COALESCE(_s1.sum_o_totalprice, 0) AS total_recent_value
FROM tpch.customer AS customer
JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
ORDER BY
  2 DESC NULLS LAST
LIMIT 3
