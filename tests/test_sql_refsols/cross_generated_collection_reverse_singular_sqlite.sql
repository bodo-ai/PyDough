SELECT
  nation.n_nationkey AS k,
  one_row.column1 AS x
FROM tpch.nation AS nation
CROSS JOIN (VALUES
  (0)) AS one_row
WHERE
  nation.n_nationkey <= 1
ORDER BY
  1
