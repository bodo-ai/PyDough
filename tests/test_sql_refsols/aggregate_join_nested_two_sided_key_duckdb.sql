WITH _s2 AS (
  SELECT
    c_nationkey,
    COUNT(*) AS n_rows
  FROM main.customer
  GROUP BY
    1
)
SELECT
  CASE
    WHEN ' - ' IS NULL OR nation.n_name IS NULL OR region.r_name IS NULL
    THEN NULL
    ELSE CONCAT_WS(' - ', nation.n_name, region.r_name)
  END AS label,
  SUM(_s2.n_rows) AS n_customers
FROM _s2 AS _s2
JOIN main.nation AS nation
  ON _s2.c_nationkey = nation.n_nationkey
JOIN main.region AS region
  ON nation.n_regionkey = region.r_regionkey
GROUP BY
  1
ORDER BY
  CASE
    WHEN nation.n_name IS NULL OR region.r_name IS NULL
    THEN NULL
    ELSE CONCAT_WS(' - ', nation.n_name, region.r_name)
  END NULLS FIRST
