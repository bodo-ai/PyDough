WITH _s3 AS (
  SELECT
    column1 AS x
  FROM (VALUES
    (0)) AS one_row(x)
), _t0 AS (
  SELECT DISTINCT
    region.r_regionkey
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN _s3 AS _s3
    ON _s3.x <= region.r_regionkey
)
SELECT
  r_regionkey AS rk
FROM _t0
ORDER BY
  1
