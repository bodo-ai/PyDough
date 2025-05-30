WITH _t0 AS (
  SELECT
    DATEDIFF(
      o_orderdate,
      LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS LAST),
      DAY
    ) AS day_diff,
    o_custkey AS customer_key
  FROM tpch.orders
), _s1 AS (
  SELECT
    AVG(day_diff) AS avg_diff,
    customer_key
  FROM _t0
  GROUP BY
    customer_key
)
SELECT
  customer.c_name AS name,
  _s1.avg_diff
FROM tpch.customer AS customer
JOIN _s1 AS _s1
  ON _s1.customer_key = customer.c_custkey
ORDER BY
  avg_diff DESC
LIMIT 5
