WITH _s1 AS (
  SELECT
    n_regionkey AS region_key,
    COLLECT_LIST(n_name) AS nation_names
  FROM tpch.nation
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s1.nation_names,
  _s2.val AS nation_name
FROM tpch.region AS region
JOIN _s1 AS _s1
  ON _s1.region_key = region.r_regionkey
CROSS JOIN LATERAL POSEXPLODE(_s1.nation_names) AS _s2(idx, val)
ORDER BY
  1,
  3
