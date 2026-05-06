WITH "_u_0" AS (
  SELECT
    c_nationkey AS "_u_1"
  FROM TPCH.CUSTOMER
  WHERE
    c_mktsegment = 'BUILDING'
  GROUP BY
    c_nationkey
)
SELECT
  COUNT(*) AS n
FROM TPCH.NATION NATION
JOIN TPCH.REGION REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'EUROPE'
LEFT JOIN "_u_0" "_u_0"
  ON NATION.n_nationkey = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
