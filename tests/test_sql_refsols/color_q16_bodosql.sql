WITH _t1 AS (
  SELECT
    ARRAY_AGG(identname) AS listof_identname,
    chex,
    COUNT(*) AS n_rows
  FROM clrs
  GROUP BY
    2
)
SELECT
  chex AS hex_code,
  listof_identname AS names
FROM _t1
WHERE
  n_rows >= 3
ORDER BY
  1 NULLS FIRST
