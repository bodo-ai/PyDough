WITH _t0 AS (
  SELECT DISTINCT
    REGION.r_regionkey
  FROM tpch.REGION AS REGION
  JOIN tpch.NATION AS NATION
    ON NATION.n_regionkey = REGION.r_regionkey
  JOIN (VALUES
    ROW(0)) AS one_row(x)
    ON REGION.r_regionkey >= one_row.x
)
SELECT
  r_regionkey AS rk
FROM _t0
ORDER BY
  1
