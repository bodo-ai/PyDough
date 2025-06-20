WITH _T0 AS (
  SELECT
    SUM(
      IFF(
        NATION_2.n_name = 'BRAZIL',
        LINEITEM.l_extendedprice * (
          1 - LINEITEM.l_discount
        ),
        0
      )
    ) AS AGG_0,
    SUM(LINEITEM.l_extendedprice * (
      1 - LINEITEM.l_discount
    )) AS AGG_1,
    DATE_PART(YEAR, ORDERS.o_orderdate) AS O_YEAR
  FROM TPCH.LINEITEM AS LINEITEM
  JOIN TPCH.PART AS PART
    ON LINEITEM.l_partkey = PART.p_partkey AND PART.p_type = 'ECONOMY ANODIZED STEEL'
  JOIN TPCH.ORDERS AS ORDERS
    ON DATE_PART(YEAR, ORDERS.o_orderdate) IN (1995, 1996)
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
    DATE_PART(YEAR, ORDERS.o_orderdate)
)
SELECT
  O_YEAR,
  COALESCE(AGG_0, 0) / COALESCE(AGG_1, 0) AS MKT_SHARE
FROM _T0
