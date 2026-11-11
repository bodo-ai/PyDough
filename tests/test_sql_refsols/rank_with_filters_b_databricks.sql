WITH _t0 AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY c_acctbal DESC NULLS FIRST, c_name NULLS LAST) AS r,
    c_name
  FROM tpch_s3_pq.tpch_sf1.customer
)
SELECT
  c_name AS n,
  r
FROM _t0
WHERE
  ENDSWITH(c_name, '0') AND r <= 30
