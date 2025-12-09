WITH _s1 AS (
  SELECT
    n_name,
    n_regionkey
  FROM tpch.NATION
  WHERE
    n_nationkey = 4
)
SELECT
  REGION.r_name AS name,
  _s1.n_name AS nation_4_name
FROM tpch.REGION AS REGION
LEFT JOIN _s1 AS _s1
  ON REGION.r_regionkey = _s1.n_regionkey
