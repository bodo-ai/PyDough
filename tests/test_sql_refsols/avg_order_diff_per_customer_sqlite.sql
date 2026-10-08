WITH _s3 AS (
  SELECT
    o_custkey,
    CAST((
      JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(
        DATE(
          LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate),
          'start of day'
        )
      )
    ) AS INTEGER) AS day_diff
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  MAX(customer.c_name) AS name,
  AVG(_s3.day_diff) AS avg_diff
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'JAPAN'
JOIN _s3 AS _s3
  ON _s3.o_custkey = customer.c_custkey
GROUP BY
  _s3.o_custkey
ORDER BY
  2 DESC
LIMIT 5
