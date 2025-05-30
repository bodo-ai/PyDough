WITH _t1 AS (
  SELECT
    o_custkey AS customer_key,
    o_orderdate AS order_date,
    o_totalprice AS total_price
  FROM tpch.orders
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS LAST, o_orderkey NULLS LAST) = 1
)
SELECT
  customer.c_name AS name,
  _t1.order_date AS first_order_date,
  _t1.total_price AS first_order_price
FROM tpch.customer AS customer
JOIN _t1 AS _t1
  ON _t1.customer_key = customer.c_custkey
WHERE
  customer.c_acctbal >= 9000.0
ORDER BY
  first_order_price DESC
LIMIT 5
