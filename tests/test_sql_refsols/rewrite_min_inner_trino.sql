WITH _t0 AS (
  SELECT
    o_custkey AS c_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  WHERE
    YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1994 AND o_orderpriority = '1-URGENT'
  GROUP BY
    1
)
SELECT
  MIN(c_custkey) AS min_k,
  MAX(c_custkey) AS max_k,
  SUM(n_rows) AS n
FROM _t0
