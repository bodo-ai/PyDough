WITH "_u_0" AS (
  SELECT
    nation_key AS "_u_1"
  FROM ASIAN_NATIONS_T5
  GROUP BY
    nation_key
)
SELECT
  customer.c_name AS name
FROM TPCH.CUSTOMER CUSTOMER
LEFT JOIN "_u_0" "_u_0"
  ON CUSTOMER.c_nationkey = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
