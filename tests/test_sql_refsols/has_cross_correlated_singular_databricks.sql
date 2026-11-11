WITH _u_0 AS (
  SELECT
    n_nationkey AS _u_1
  FROM tpch_s3_pq.tpch_sf1.nation
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch_s3_pq.tpch_sf1.customer AS customer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customer.c_nationkey
WHERE
  NOT _u_0._u_1 IS NULL
