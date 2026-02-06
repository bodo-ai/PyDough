WITH _u_0 AS (
  SELECT
    NATION.n_nationkey AS _u_1
  FROM tpch.NATION AS NATION
  JOIN tpch.REGION AS REGION
    ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AFRICA'
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.SUPPLIER AS SUPPLIER
LEFT JOIN _u_0 AS _u_0
  ON SUPPLIER.s_nationkey = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
