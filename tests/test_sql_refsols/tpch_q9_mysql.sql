SELECT
  NATION.n_name AS NATION,
  EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) AS O_YEAR,
  COALESCE(
    SUM(
      LINEITEM.l_extendedprice * (
        1 - LINEITEM.l_discount
      ) - PARTSUPP.ps_supplycost * LINEITEM.l_quantity
    ),
    0
  ) AS AMOUNT
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.PART AS PART
  ON LINEITEM.l_partkey = PART.p_partkey AND PART.p_name LIKE '%green%'
JOIN tpch.SUPPLIER AS SUPPLIER
  ON LINEITEM.l_suppkey = SUPPLIER.s_suppkey
JOIN tpch.NATION AS NATION
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN tpch.ORDERS AS ORDERS
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
JOIN tpch.PARTSUPP AS PARTSUPP
  ON LINEITEM.l_partkey = PARTSUPP.ps_partkey
  AND LINEITEM.l_suppkey = PARTSUPP.ps_suppkey
GROUP BY
  1,
  2
ORDER BY
  1 COLLATE utf8mb4_bin,
  2 DESC
LIMIT 10
