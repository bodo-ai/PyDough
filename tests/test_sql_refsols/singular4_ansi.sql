WITH _t2 AS (
  SELECT
    o_custkey,
    o_orderdate
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC NULLS FIRST) = 1
)
SELECT
  customer.c_name AS name
FROM tpch.customer AS customer
LEFT JOIN _t2 AS _t2
  ON _t2.o_custkey = customer.c_custkey
WHERE
  customer.c_nationkey = 6
ORDER BY
  _t2.o_orderdate NULLS LAST
LIMIT 5
