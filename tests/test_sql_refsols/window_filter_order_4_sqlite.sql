WITH _t2 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'GERMANY'
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1992
    AND customer.c_custkey = orders.o_custkey
  GROUP BY
    orders.o_custkey
), _t AS (
  SELECT
    n_rows,
    AVG(n_rows) OVER () AS _w
  FROM _t2
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  _w > n_rows
