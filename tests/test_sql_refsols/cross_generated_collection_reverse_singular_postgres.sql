SELECT
  nation.n_nationkey AS k,
  one_row.x
FROM tpch.nation AS nation
CROSS JOIN GENERATE_SERIES(0, 0, 1) AS one_row(x)
WHERE
  nation.n_nationkey <= 1
ORDER BY
  1 NULLS FIRST
