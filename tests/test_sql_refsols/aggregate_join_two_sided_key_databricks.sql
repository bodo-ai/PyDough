SELECT
  CASE
    WHEN ' - ' IS NULL OR customer.c_mktsegment IS NULL OR nation.n_name IS NULL
    THEN NULL
    ELSE CONCAT_WS(' - ', customer.c_mktsegment, nation.n_name)
  END AS label,
  COUNT(*) AS n
FROM tpch_s3_pq.tpch_sf1.customer AS customer
JOIN tpch_s3_pq.tpch_sf1.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
GROUP BY
  1
ORDER BY
  CASE
    WHEN customer.c_mktsegment IS NULL OR nation.n_name IS NULL
    THEN NULL
    ELSE CONCAT_WS(' - ', customer.c_mktsegment, nation.n_name)
  END
