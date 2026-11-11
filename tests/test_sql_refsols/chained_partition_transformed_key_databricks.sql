WITH _t0 AS (
  SELECT DISTINCT
    LOWER(n_name) AS lower_n_name
  FROM tpch_s3_pq.tpch_sf1.nation
)
SELECT
  lower_n_name AS name
FROM _t0
ORDER BY
  1
