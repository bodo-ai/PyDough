WITH _t0 AS (
  SELECT DISTINCT
    region.r_regionkey
  FROM tpch_s3_pq.tpch_sf1.region AS region
  JOIN tpch_s3_pq.tpch_sf1.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN VALUES
    (0) AS one_row(x)
    ON one_row.x <= region.r_regionkey
)
SELECT
  r_regionkey AS rk
FROM _t0
ORDER BY
  1
