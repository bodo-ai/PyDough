WITH _u_0 AS (
  SELECT
    c_nationkey AS _u_1
  FROM tpch.CUSTOMER
  WHERE
    c_mktsegment = 'BUILDING'
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.NATION AS NATION
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'EUROPE'
LEFT JOIN _u_0 AS _u_0
  ON NATION.n_nationkey = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
