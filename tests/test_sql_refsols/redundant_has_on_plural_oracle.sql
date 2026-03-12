WITH "_u_0" AS (
  SELECT
    o_custkey AS "_u_1"
  FROM TPCH.ORDERS
  WHERE
    o_totalprice > 400000
  GROUP BY
    o_custkey
)
SELECT
  COUNT(*) AS n
FROM TPCH.CUSTOMER CUSTOMER
LEFT JOIN "_u_0" "_u_0"
  ON CUSTOMER.c_custkey = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
