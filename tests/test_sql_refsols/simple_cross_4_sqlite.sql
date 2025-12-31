WITH _s2 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
), _s3 AS (
  SELECT
    _s0.r_regionkey,
    COUNT(*) AS n_rows
  FROM _s2 AS _s0
  JOIN tpch.region AS region
    ON SUBSTRING(_s0.r_name, 1, 1) = SUBSTRING(region.r_name, 1, 1)
    AND _s0.r_name <> region.r_name
  GROUP BY
    1
)
SELECT
  _s2.r_name AS region_name,
  COALESCE(_s3.n_rows, 0) AS n_other_regions
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.r_regionkey = _s3.r_regionkey
ORDER BY
  1
