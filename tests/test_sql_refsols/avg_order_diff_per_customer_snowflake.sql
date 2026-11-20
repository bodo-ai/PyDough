WITH _s3 AS (
  SELECT
    o_custkey,
    DATEDIFF(
      DAY,
      CAST(LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS DATETIME),
      CAST(o_orderdate AS DATETIME)
    ) AS day_diff
  FROM snowflake_sample_data.tpch_sf1.orders
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  ANY_VALUE(customer.c_name) AS name,
  AVG(_s3.day_diff) AS avg_diff
FROM snowflake_sample_data.tpch_sf1.customer AS customer
JOIN snowflake_sample_data.tpch_sf1.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'JAPAN'
JOIN _s3 AS _s3
  ON _s3.o_custkey = customer.c_custkey
GROUP BY
  _s3.o_custkey
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
