WITH "_u_0" AS (
  SELECT
    NATION.n_nationkey AS "_u_1"
  FROM TPCH.NATION NATION
  JOIN TPCH.REGION REGION
    ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AFRICA'
  GROUP BY
    NATION.n_nationkey
)
SELECT
  COUNT(*) AS n
FROM TPCH.SUPPLIER SUPPLIER
LEFT JOIN "_u_0" "_u_0"
  ON SUPPLIER.s_nationkey = "_u_0"."_u_1"
WHERE
  "_u_0"."_u_1" IS NULL
