WITH _s1 AS (
  SELECT
    column1 AS priority_lvl,
    column2 AS tax_rate
  FROM (VALUES
    ('1-URGENT', 0.05),
    ('2-HIGH', 0.04),
    ('3-MEDIUM', 0.03),
    ('4-NOT SPECIFIED', 0.02)) AS priority_taxes(priority_lvl, tax_rate)
), _t1 AS (
  SELECT
    orders.o_custkey,
    orders.o_orderkey,
    orders.o_orderpriority,
    orders.o_totalprice,
    _s1.tax_rate
  FROM tpch.orders AS orders
  JOIN _s1 AS _s1
    ON _s1.priority_lvl = orders.o_orderpriority
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_orderkey ORDER BY orders.o_totalprice + orders.o_totalprice * _s1.tax_rate NULLS LAST) = 1
)
SELECT
  customer.c_name AS name,
  _t1.o_orderkey AS order_key,
  _t1.o_orderpriority AS order_priority,
  _t1.o_totalprice + _t1.o_totalprice * _t1.tax_rate AS cheapest_order_price
FROM tpch.customer AS customer
JOIN _t1 AS _t1
  ON _t1.o_custkey = customer.c_custkey
ORDER BY
  4
LIMIT 5
