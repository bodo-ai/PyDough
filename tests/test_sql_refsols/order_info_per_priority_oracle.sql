WITH "_T" AS (
  SELECT
    o_orderkey AS O_ORDERKEY,
    o_orderpriority AS O_ORDERPRIORITY,
    o_totalprice AS O_TOTALPRICE,
    ROW_NUMBER() OVER (PARTITION BY o_orderpriority ORDER BY o_totalprice DESC) AS "_W"
  FROM TPCH.ORDERS
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) = 1992
)
SELECT
  O_ORDERPRIORITY AS order_priority,
  O_ORDERKEY AS order_key,
  O_TOTALPRICE AS order_total_price
FROM "_T"
WHERE
  "_W" = 1
ORDER BY
  1 NULLS FIRST
