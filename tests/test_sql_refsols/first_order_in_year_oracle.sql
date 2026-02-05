WITH _T AS (
  SELECT
    o_orderdate AS O_ORDERDATE,
    o_orderkey AS O_ORDERKEY,
    LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS _W,
    LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS _W_2
  FROM TPCH.ORDERS
  WHERE
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 1
)
SELECT
  O_ORDERDATE AS order_date,
  O_ORDERKEY AS key
FROM _T
WHERE
  EXTRACT(YEAR FROM CAST(O_ORDERDATE AS DATETIME)) <> EXTRACT(YEAR FROM CAST(_W_2 AS DATETIME))
  OR _W IS NULL
ORDER BY
  1 NULLS FIRST
