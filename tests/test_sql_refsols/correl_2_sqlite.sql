WITH _s3 AS (
  SELECT
    c_comment,
    c_nationkey
  FROM tpch.customer
), _t0 AS (
  SELECT
    _s3.c_nationkey,
    MAX(nation.n_name) AS anything_n_name,
    COUNT(*) AS n_rows
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  LEFT JOIN _s3 AS _s3
    ON LOWER(SUBSTRING(region.r_name, 1, 1)) = SUBSTRING(_s3.c_comment, 1, 1)
    AND _s3.c_nationkey = nation.n_nationkey
  WHERE
    NOT region.r_name LIKE 'A%'
  GROUP BY
    1,
    SUBSTRING(_s3.c_comment, 1, 1)
)
SELECT
  anything_n_name AS name,
  n_rows * IIF(NOT c_nationkey IS NULL, 1, 0) AS n_selected_custs
FROM _t0
ORDER BY
  1
