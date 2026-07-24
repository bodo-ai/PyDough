WITH "_T0" AS (
  SELECT
    o_custkey AS C_CUSTKEY,
    COUNT(*) AS N_ROWS
  FROM TPCH.ORDERS
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) = 1994
    AND o_orderpriority = '1-URGENT'
  GROUP BY
    o_custkey
)
SELECT
  MIN(C_CUSTKEY) AS min_k,
  MAX(C_CUSTKEY) AS max_k,
  SUM(N_ROWS) AS n
FROM "_T0"
