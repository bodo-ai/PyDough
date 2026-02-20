WITH _t AS (
  SELECT
    ORDERS.o_custkey,
    ORDERS.o_orderkey,
    ORDERS.o_orderpriority,
    ORDERS.o_totalprice,
    priority_taxes.tax_rate,
    ROW_NUMBER() OVER (PARTITION BY ORDERS.o_orderkey ORDER BY CASE
      WHEN ORDERS.o_totalprice + ORDERS.o_totalprice * priority_taxes.tax_rate IS NULL
      THEN 1
      ELSE 0
    END, ORDERS.o_totalprice + ORDERS.o_totalprice * priority_taxes.tax_rate) AS _w
  FROM tpch.ORDERS AS ORDERS
  JOIN (VALUES
    ROW('1-URGENT', 0.05),
    ROW('2-HIGH', 0.04),
    ROW('3-MEDIUM', 0.03),
    ROW('4-NOT SPECIFIED', 0.02)) AS priority_taxes(priority_lvl, tax_rate)
    ON ORDERS.o_orderpriority = priority_taxes.priority_lvl
)
SELECT
  CUSTOMER.c_name AS name,
  _t.o_orderkey AS order_key,
  _t.o_orderpriority AS order_priority,
  _t.o_totalprice + _t.o_totalprice * _t.tax_rate AS cheapest_order_price
FROM tpch.CUSTOMER AS CUSTOMER
JOIN _t AS _t
  ON CUSTOMER.c_custkey = _t.o_custkey AND _t._w = 1
ORDER BY
  4
LIMIT 5
