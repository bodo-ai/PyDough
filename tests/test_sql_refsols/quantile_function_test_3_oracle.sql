WITH "_S0" AS (
  SELECT
    n_name AS N_NAME,
    n_nationkey AS N_NATIONKEY,
    n_regionkey AS N_REGIONKEY
  FROM TPCH.NATION
  ORDER BY
    1 NULLS FIRST
  FETCH FIRST 5 ROWS ONLY
), "_S5" AS (
  SELECT
    CUSTOMER.c_nationkey AS C_NATIONKEY,
    ORDERS.o_totalprice AS O_TOTALPRICE,
    CASE
      WHEN FLOOR(0.99 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC NULLS LAST)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS EXPR_10,
    CASE
      WHEN FLOOR(0.75 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC NULLS LAST)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS EXPR_11,
    CASE
      WHEN FLOOR(0.25 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC NULLS LAST)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS EXPR_12,
    CASE
      WHEN FLOOR(0.1 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC NULLS LAST)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS EXPR_13,
    CASE
      WHEN FLOOR(0.01 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC NULLS LAST)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS EXPR_14,
    CASE
      WHEN FLOOR(0.5 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC NULLS LAST)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS EXPR_15,
    CASE
      WHEN FLOOR(0.9 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC NULLS LAST)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS EXPR_9
  FROM TPCH.CUSTOMER CUSTOMER
  JOIN TPCH.ORDERS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
    AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATE)) = 1998
)
SELECT
  ANY_VALUE(REGION.r_name) AS region_name,
  ANY_VALUE("_S0".N_NAME) AS nation_name,
  MIN("_S5".O_TOTALPRICE) AS orders_min,
  MAX("_S5".EXPR_10) AS orders_1_percent,
  MAX("_S5".EXPR_9) AS orders_10_percent,
  MAX("_S5".EXPR_11) AS orders_25_percent,
  MAX("_S5".EXPR_15) AS orders_median,
  MAX("_S5".EXPR_12) AS orders_75_percent,
  MAX("_S5".EXPR_13) AS orders_90_percent,
  MAX("_S5".EXPR_14) AS orders_99_percent,
  MAX("_S5".O_TOTALPRICE) AS orders_max
FROM "_S0" "_S0"
JOIN TPCH.REGION REGION
  ON REGION.r_regionkey = "_S0".N_REGIONKEY
LEFT JOIN "_S5" "_S5"
  ON "_S0".N_NATIONKEY = "_S5".C_NATIONKEY
GROUP BY
  "_S0".N_NATIONKEY
ORDER BY
  2 NULLS FIRST
