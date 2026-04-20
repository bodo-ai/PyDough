WITH "_u_0" AS (
  SELECT
    o_custkey AS "_u_1"
  FROM TPCH.ORDERS
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) = 1994
    AND o_orderpriority = '1-URGENT'
  GROUP BY
    o_custkey
)
SELECT
  ANY_VALUE(CUSTOMER.c_name) AS any
FROM TPCH.CUSTOMER CUSTOMER
LEFT JOIN "_u_0" "_u_0"
  ON CUSTOMER.c_custkey = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
