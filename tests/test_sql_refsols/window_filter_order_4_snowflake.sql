WITH _t2 AS (
  SELECT
    MAX(1) AS "_"
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'GERMANY'
  JOIN tpch.orders AS orders
    ON YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1992
    AND customer.c_custkey = orders.o_custkey
  GROUP BY
    orders.o_custkey
), _t1 AS (
  SELECT
    1 AS "_"
  FROM _t2
  QUALIFY
    n_rows < AVG(n_rows) OVER ()
)
SELECT
  COUNT(*) AS n
FROM _t1
