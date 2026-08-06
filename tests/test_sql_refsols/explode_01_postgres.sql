WITH _s1 AS (
  SELECT
    ARRAY_AGG(n_name) AS nation_names,
    n_regionkey AS region_key
  FROM tpch.nation
  GROUP BY
    2
)
SELECT
  region.r_name AS region_name,
  _s1.nation_names,
  _l.idx - 1 AS nation_idx,
  _l.val AS nation_name
FROM tpch.region AS region
JOIN _s1 AS _s1
  ON _s1.region_key = region.r_regionkey
CROSS JOIN LATERAL UNNEST(_s1.nation_names) WITH ORDINALITY AS _l(val, idx)
ORDER BY
  1 NULLS FIRST,
  3 NULLS FIRST
