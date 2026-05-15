WITH _s0 AS (
  SELECT
    n_name,
    n_nationkey,
    n_regionkey
  FROM tpch.nation
  ORDER BY
    1
  LIMIT 8
)
SELECT
  _s0.n_nationkey AS key,
  _s0.n_name AS name
FROM _s0 AS _s0
JOIN tpch.region AS region
  ON _s0.n_regionkey = region.r_regionkey AND region.r_name <> 'AMERICA'
ORDER BY
  2
LIMIT 3
