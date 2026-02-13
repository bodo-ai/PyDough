WITH _t2 AS (
  SELECT
    lineitem.l_receiptdate,
    lineitem.l_suppkey,
    orders.o_custkey
  FROM tpch.orders AS orders
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
  WHERE
    orders.o_clerk = 'Clerk#000000017'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY orders.o_custkey ORDER BY lineitem.l_receiptdate, lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    ) DESC) = 1
)
SELECT
  customer.c_name AS name,
  _t2.l_receiptdate AS receipt_date,
  nation.n_name AS nation_name
FROM tpch.customer AS customer
JOIN _t2 AS _t2
  ON _t2.o_custkey = customer.c_custkey
JOIN tpch.supplier AS supplier
  ON _t2.l_suppkey = supplier.s_suppkey
JOIN tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
WHERE
  customer.c_nationkey = 4
ORDER BY
  2 NULLS FIRST,
  1 NULLS FIRST
LIMIT 5
