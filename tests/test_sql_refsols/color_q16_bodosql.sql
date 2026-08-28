WITH _t1 AS (
  SELECT
    chex,
    ARRAY_AGG(identname) AS listof_identname,
    COUNT(*) AS n_rows
  FROM clrs
  GROUP BY
    1
)
SELECT
  chex AS hex_code,
  listof_identname AS names
FROM _t1
WHERE
  n_rows >= 3
ORDER BY
  1 NULLS FIRST
