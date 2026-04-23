WITH "_S1" AS (
  SELECT DISTINCT
    c_custkey AS C_CUSTKEY
  FROM TPCH.CUSTOMER
  WHERE
    c_mktsegment = 'BUILDING'
), "_u_0" AS (
  SELECT
    C_CUSTKEY AS "_u_1"
  FROM "_S1"
  GROUP BY
    C_CUSTKEY
), "_T" AS (
  SELECT
    ORDERS.o_totalprice AS O_TOTALPRICE,
    AVG(CAST(NULL AS INT)) OVER () AS "_W"
  FROM TPCH.ORDERS ORDERS
  LEFT JOIN "_u_0" "_u_0"
    ON ORDERS.o_custkey = "_u_0"."_u_1"
  WHERE
    ORDERS.o_clerk = 'Clerk#000000001' AND "_u_0"."_u_1" IS NULL
)
SELECT
  COUNT(*) AS n
FROM "_T"
WHERE
  O_TOTALPRICE < (
    0.05 * "_W"
  )
