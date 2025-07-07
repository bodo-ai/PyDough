SELECT
  DATE_PART(YEAR, CAST(ORDERS.o_orderdate AS DATETIME)) AS O_YEAR,
  COALESCE(
    SUM(
      IFF(
        NATION_2.n_name = 'BRAZIL',
        LINEITEM.l_extendedprice * (
          1 - LINEITEM.l_discount
        ),
        0
      )
    ),
    0
  ) / COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS MKT_SHARE
FROM TPCH.LINEITEM AS LINEITEM
JOIN TPCH.PART AS PART
  ON LINEITEM.l_partkey = PART.p_partkey AND PART.p_type = 'ECONOMY ANODIZED STEEL'
JOIN TPCH.ORDERS AS ORDERS
  ON DATE_PART(YEAR, CAST(ORDERS.o_orderdate AS DATETIME)) IN (1995, 1996)
  AND LINEITEM.l_orderkey = ORDERS.o_orderkey
JOIN TPCH.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = ORDERS.o_custkey
JOIN TPCH.NATION AS NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
JOIN TPCH.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
JOIN TPCH.SUPPLIER AS SUPPLIER
  ON LINEITEM.l_suppkey = SUPPLIER.s_suppkey
JOIN TPCH.NATION AS NATION_2
  ON NATION_2.n_nationkey = SUPPLIER.s_nationkey
GROUP BY
  DATE_PART(YEAR, CAST(ORDERS.o_orderdate AS DATETIME))
