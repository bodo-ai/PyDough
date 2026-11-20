SELECT
  NATION.n_nationkey AS k,
  one_row.x
FROM tpch.NATION AS NATION
CROSS JOIN (VALUES
  ROW(0)) AS one_row(x)
WHERE
  NATION.n_nationkey <= 1
ORDER BY
  1
