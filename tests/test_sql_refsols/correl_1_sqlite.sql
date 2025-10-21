WITH _s1 AS (
  SELECT
    n_name,
    n_regionkey
  FROM tpch.nation
), _t0 AS (
  SELECT
    _s1.n_regionkey,
    MAX(region.r_name) AS anything_r_name,
    COUNT(*) AS n_rows
  FROM tpch.region AS region
  LEFT JOIN _s1 AS _s1
    ON SUBSTRING(_s1.n_name, 1, 1) = SUBSTRING(region.r_name, 1, 1)
    AND _s1.n_regionkey = region.r_regionkey
  GROUP BY
    1,
    SUBSTRING(_s1.n_name, 1, 1)
)
SELECT
  anything_r_name AS region_name,
  n_rows * IIF(NOT n_regionkey IS NULL, 1, 0) AS n_prefix_nations
FROM _t0
ORDER BY
  1
