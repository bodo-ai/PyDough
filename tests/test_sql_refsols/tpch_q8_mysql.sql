SELECT
  EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) AS O_YEAR,
  COALESCE(
    SUM(
      CASE
        WHEN NATION_2.n_name = 'BRAZIL'
        THEN LINEITEM.l_extendedprice * (
          1 - LINEITEM.l_discount
        )
        ELSE 0
      END
    ),
    0
  ) / COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS MKT_SHARE
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.PART AS PART
  ON LINEITEM.l_partkey = PART.p_partkey AND PART.p_type = 'ECONOMY ANODIZED STEEL'
JOIN tpch.ORDERS AS ORDERS
  ON EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) IN (1995, 1996)
  AND LINEITEM.l_orderkey = ORDERS.o_orderkey
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = ORDERS.o_custkey
JOIN tpch.NATION AS NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
JOIN tpch.SUPPLIER AS SUPPLIER
  ON LINEITEM.l_suppkey = SUPPLIER.s_suppkey
JOIN tpch.NATION AS NATION_2
  ON NATION_2.n_nationkey = SUPPLIER.s_nationkey
GROUP BY
  1
