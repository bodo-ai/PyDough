WITH _s3 AS (
  SELECT
    SUBSTRING(c_comment, 1, 1) AS expr_1,
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.customer
  GROUP BY
    1,
    2
)
SELECT
  nation.n_name AS name,
  COALESCE(_s3.n_rows, 0) AS n_selected_custs
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
LEFT JOIN _s3 AS _s3
  ON _s3.c_nationkey = nation.n_nationkey
  AND _s3.expr_1 = LOWER(SUBSTRING(region.r_name, 1, 1))
WHERE
  NOT region.r_name LIKE 'A%'
ORDER BY
  1
