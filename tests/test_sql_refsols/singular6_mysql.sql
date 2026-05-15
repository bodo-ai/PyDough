WITH _t AS (
  SELECT
    LINEITEM.l_receiptdate,
    LINEITEM.l_suppkey,
    ORDERS.o_custkey,
    ROW_NUMBER() OVER (PARTITION BY ORDERS.o_custkey ORDER BY CASE WHEN LINEITEM.l_receiptdate IS NULL THEN 1 ELSE 0 END, LINEITEM.l_receiptdate, CASE
      WHEN LINEITEM.l_extendedprice * (
        1 - LINEITEM.l_discount
      ) IS NULL
      THEN 1
      ELSE 0
    END DESC, LINEITEM.l_extendedprice * (
      1 - LINEITEM.l_discount
    ) DESC) AS _w
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_orderkey = ORDERS.o_orderkey
  WHERE
    ORDERS.o_clerk = 'Clerk#000000017'
)
SELECT
  CUSTOMER.c_name COLLATE utf8mb4_bin AS name,
  _t.l_receiptdate AS receipt_date,
  NATION.n_name AS nation_name
FROM tpch.CUSTOMER AS CUSTOMER
JOIN _t AS _t
  ON CUSTOMER.c_custkey = _t.o_custkey AND _t._w = 1
JOIN tpch.SUPPLIER AS SUPPLIER
  ON SUPPLIER.s_suppkey = _t.l_suppkey
JOIN tpch.NATION AS NATION
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
WHERE
  CUSTOMER.c_nationkey = 4
ORDER BY
  2,
  1
LIMIT 5
