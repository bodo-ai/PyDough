WITH _s1 AS (
  SELECT
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.customer
  GROUP BY
    1
)
SELECT
  nation.n_name AS nation_name,
  ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY _s1.n_rows DESC NULLS FIRST, region.r_name NULLS LAST) AS rank
FROM tpch.nation AS nation
JOIN _s1 AS _s1
  ON _s1.c_nationkey = nation.n_nationkey
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey
ORDER BY
  2
LIMIT 5
