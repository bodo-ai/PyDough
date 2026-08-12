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
  _s2.idx AS nation_idx,
  _s2.val AS nation_name
FROM tpch.region AS region
JOIN _s1 AS _s1
  ON _s1.region_key = region.r_regionkey
CROSS JOIN LATERAL (
  SELECT
    UNNEST(_s1.nation_names) AS _col_0,
    GENERATE_SUBSCRIPTS(_s1.nation_names, 1) - 1 AS _col_1
) AS _s2(val, idx)
ORDER BY
  1 NULLS FIRST,
  3 NULLS FIRST
