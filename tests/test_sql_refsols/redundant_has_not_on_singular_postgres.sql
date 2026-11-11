WITH _u_0 AS (
  SELECT
    nation.n_nationkey AS _u_1
  FROM main.nation AS nation
  JOIN main.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AFRICA'
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM main.supplier AS supplier
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = supplier.s_nationkey
WHERE
  _u_0._u_1 IS NULL
