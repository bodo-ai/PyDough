WITH "_S6" AS (
  SELECT
    PARTSUPP.ps_partkey AS PS_PARTKEY,
    SUM(LINEITEM.l_quantity) AS SUM_L_QUANTITY,
    SUM(
      LINEITEM.l_extendedprice * (
        1 - LINEITEM.l_discount
      ) * (
        1 - LINEITEM.l_tax
      ) - LINEITEM.l_quantity * PARTSUPP.ps_supplycost
    ) AS SUM_REVENUE
  FROM TPCH.PARTSUPP PARTSUPP
  JOIN TPCH.SUPPLIER SUPPLIER
    ON PARTSUPP.ps_suppkey = SUPPLIER.s_suppkey
    AND SUPPLIER.s_name = 'Supplier#000000182'
  JOIN TPCH.PART PART
    ON PART.p_container LIKE 'MED%' AND PART.p_partkey = PARTSUPP.ps_partkey
  JOIN TPCH.LINEITEM LINEITEM
    ON EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATE)) = 1994
    AND LINEITEM.l_partkey = PARTSUPP.ps_partkey
    AND LINEITEM.l_suppkey = PARTSUPP.ps_suppkey
  GROUP BY
    PARTSUPP.ps_partkey,
    PARTSUPP.ps_suppkey
)
SELECT
  PART.p_name AS part_name,
  ROUND(COALESCE("_S6".SUM_REVENUE, 0) / COALESCE("_S6".SUM_L_QUANTITY, 0), 2) AS revenue_ratio
FROM "_S6" "_S6"
JOIN TPCH.PART PART
  ON PART.p_partkey = "_S6".PS_PARTKEY
ORDER BY
  2 NULLS FIRST,
  1 NULLS FIRST
FETCH FIRST 3 ROWS ONLY
