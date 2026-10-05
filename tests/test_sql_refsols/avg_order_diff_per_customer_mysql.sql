WITH _s3 AS (
  SELECT
    o_custkey,
    DATEDIFF(
      o_orderdate,
      LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate)
    ) AS day_diff
  FROM tpch.ORDERS
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  ANY_VALUE(CUSTOMER.c_name) AS name,
  AVG(_s3.day_diff) AS avg_diff
FROM tpch.CUSTOMER AS CUSTOMER
JOIN tpch.NATION AS NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey AND NATION.n_name = 'JAPAN'
JOIN _s3 AS _s3
  ON CUSTOMER.c_custkey = _s3.o_custkey
GROUP BY
  _s3.o_custkey
ORDER BY
  2 DESC
LIMIT 5
