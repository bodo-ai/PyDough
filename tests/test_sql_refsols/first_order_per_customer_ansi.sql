WITH _t2 AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_totalprice
  FROM tpch.orders
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS LAST, o_orderkey NULLS LAST) = 1
)
SELECT
  customer.c_name AS name,
  _t2.o_orderdate AS first_order_date,
  _t2.o_totalprice AS first_order_price
FROM tpch.customer AS customer
JOIN _t2 AS _t2
  ON _t2.o_custkey = customer.c_custkey
WHERE
  customer.c_acctbal >= 9000.0
ORDER BY
  3 DESC
LIMIT 5
