WITH "_u_0" AS (
  SELECT
    n_nationkey AS "_u_1"
  FROM TPCH.NATION
  GROUP BY
    n_nationkey
)
SELECT
  COUNT(*) AS n
FROM TPCH.CUSTOMER CUSTOMER
LEFT JOIN "_u_0" "_u_0"
  ON CUSTOMER.c_nationkey = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
