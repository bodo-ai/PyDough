WITH _t2 AS (
  SELECT
    o_custkey,
    o_totalprice
  FROM tpch.orders
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate DESC NULLS FIRST, o_orderkey NULLS LAST) <= 5
), _s1 AS (
  SELECT
    o_custkey,
    SUM(o_totalprice) AS sum_o_totalprice
  FROM _t2
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
  2 DESC
LIMIT 3
