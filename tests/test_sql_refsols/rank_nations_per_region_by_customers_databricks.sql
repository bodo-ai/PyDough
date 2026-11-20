WITH _s1 AS (
  SELECT
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch_s3_pq.tpch_sf1.customer
  GROUP BY
    1
)
SELECT
  nation.n_name AS nation_name,
  ROW_NUMBER() OVER (
    PARTITION BY nation.n_regionkey
    ORDER BY _s1.n_rows DESC NULLS FIRST, nation.n_name NULLS LAST
  ) AS rank
FROM tpch_s3_pq.tpch_sf1.nation AS nation
JOIN _s1 AS _s1
  ON _s1.c_nationkey = nation.n_nationkey
ORDER BY
  2
LIMIT 5
