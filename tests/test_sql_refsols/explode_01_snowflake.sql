WITH _s1 AS (
  SELECT
    n_regionkey AS region_key,
    ARRAY_AGG(n_name) AS nation_names
  FROM tpch.nation
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s1.nation_names,
  _s2.index AS nation_idx,
  _s2.value AS nation_name
FROM tpch.region AS region
JOIN _s1 AS _s1
  ON _s1.region_key = region.r_regionkey
CROSS JOIN LATERAL FLATTEN(_s1.nation_names) AS _s2(seq, key, path, index, value, this)
ORDER BY
  1 NULLS FIRST,
  4 NULLS FIRST
