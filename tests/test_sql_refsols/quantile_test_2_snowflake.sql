WITH _S0 AS (
  SELECT
    n_name AS N_NAME,
    n_nationkey AS N_NATIONKEY,
    n_regionkey AS N_REGIONKEY
  FROM TPCH.NATION
  ORDER BY
    N_NAME NULLS FIRST
  LIMIT 5
), _S5 AS (
  SELECT
    PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_0,
    PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_1,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_2,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_3,
    PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_4,
    PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_5,
    PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_6,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_7,
    PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY
      ORDERS.o_totalprice) AS AGG_8,
    CUSTOMER.c_nationkey AS C_NATIONKEY
  FROM TPCH.CUSTOMER AS CUSTOMER
  JOIN TPCH.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey AND YEAR(ORDERS.o_orderdate) = 1998
  GROUP BY
    CUSTOMER.c_nationkey
)
SELECT
  REGION.r_name AS region_name,
  _S0.N_NAME AS nation_name,
  _S5.AGG_8 AS orders_min,
  _S5.AGG_1 AS orders_1_percent,
  _S5.AGG_0 AS orders_10_percent,
  _S5.AGG_2 AS orders_25_percent,
  _S5.AGG_7 AS orders_median,
  _S5.AGG_3 AS orders_75_percent,
  _S5.AGG_4 AS orders_90_percent,
  _S5.AGG_5 AS orders_99_percent,
  _S5.AGG_6 AS orders_max
FROM _S0 AS _S0
JOIN TPCH.REGION AS REGION
  ON REGION.r_regionkey = _S0.N_REGIONKEY
LEFT JOIN _S5 AS _S5
  ON _S0.N_NATIONKEY = _S5.C_NATIONKEY
ORDER BY
  _S0.N_NAME NULLS FIRST
