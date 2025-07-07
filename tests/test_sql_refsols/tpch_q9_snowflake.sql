SELECT
  NATION.n_name AS NATION,
  YEAR(ORDERS.o_orderdate) AS O_YEAR,
  COALESCE(
    SUM(
      LINEITEM.l_extendedprice * (
        1 - LINEITEM.l_discount
      ) - PARTSUPP.ps_supplycost * LINEITEM.l_quantity
    ),
    0
  ) AS AMOUNT
FROM TPCH.LINEITEM AS LINEITEM
JOIN TPCH.PART AS PART
  ON CONTAINS(PART.p_name, 'green') AND LINEITEM.l_partkey = PART.p_partkey
JOIN TPCH.SUPPLIER AS SUPPLIER
  ON LINEITEM.l_suppkey = SUPPLIER.s_suppkey
JOIN TPCH.NATION AS NATION
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN TPCH.ORDERS AS ORDERS
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
JOIN TPCH.PARTSUPP AS PARTSUPP
  ON LINEITEM.l_partkey = PARTSUPP.ps_partkey
  AND LINEITEM.l_suppkey = PARTSUPP.ps_suppkey
GROUP BY
  NATION.n_name,
  YEAR(ORDERS.o_orderdate)
ORDER BY
  NATION.n_name NULLS FIRST,
  O_YEAR DESC NULLS LAST
LIMIT 10
