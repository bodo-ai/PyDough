WITH "_u_0" AS (
  SELECT
    c_custkey AS "_u_1"
  FROM TPCH.CUSTOMER
  WHERE
    c_mktsegment = 'BUILDING'
  GROUP BY
    c_custkey
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
