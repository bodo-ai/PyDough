WITH one_row AS (
  SELECT
    SEQ4() AS x
  FROM TABLE(GENERATOR(ROWCOUNT => 1)) AS _0
)
SELECT
  nation.n_nationkey AS k,
  one_row.x
FROM tpch.nation AS nation
CROSS JOIN one_row AS one_row
WHERE
  nation.n_nationkey <= 1
ORDER BY
  1 NULLS FIRST
