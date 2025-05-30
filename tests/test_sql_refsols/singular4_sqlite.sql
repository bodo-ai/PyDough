WITH _t AS (
  SELECT
    o_custkey AS customer_key,
    o_orderdate AS order_date,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) AS _w
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
), _s1 AS (
  SELECT
    customer_key,
    order_date
  FROM _t
  WHERE
    _w = 1
)
SELECT
  customer.c_name AS name
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.customer_key = customer.c_custkey
WHERE
  customer.c_nationkey = 6
ORDER BY
  _s1.order_date
LIMIT 5
