WITH one_row AS (
  SELECT
    SEQ4() AS x
  FROM TABLE(GENERATOR(ROWCOUNT => 1)) AS _0
), _t0 AS (
  SELECT DISTINCT
    region.r_regionkey
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN one_row AS one_row
    ON one_row.x <= region.r_regionkey
)
SELECT
  r_regionkey AS rk
FROM _t0
ORDER BY
  1 NULLS FIRST
