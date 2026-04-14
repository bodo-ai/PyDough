WITH "_T1" AS (
  SELECT
    s_name AS S_NAME,
    s_suppkey AS S_SUPPKEY
  FROM TPCH.SUPPLIER
  WHERE
    s_name = 'Supplier#000009450'
), "_S11" AS (
  SELECT
    PARTSUPP.ps_partkey AS PS_PARTKEY,
    PARTSUPP.ps_suppkey AS PS_SUPPKEY,
    SUM(
      LINEITEM.l_extendedprice * (
        1 - LINEITEM.l_discount
      ) * (
        1 - LINEITEM.l_tax
      ) - LINEITEM.l_quantity * PARTSUPP.ps_supplycost
    ) AS SUM_REVENUE
  FROM TPCH.PARTSUPP PARTSUPP
  JOIN "_T1" "_T4"
    ON PARTSUPP.ps_suppkey = "_T4".S_SUPPKEY
  JOIN TPCH.PART PART
    ON PART.p_container LIKE 'LG%' AND PART.p_partkey = PARTSUPP.ps_partkey
  JOIN TPCH.LINEITEM LINEITEM
    ON EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATE)) IN (1995, 1996)
    AND LINEITEM.l_partkey = PARTSUPP.ps_partkey
    AND LINEITEM.l_suppkey = PARTSUPP.ps_suppkey
  GROUP BY
    PARTSUPP.ps_partkey,
    PARTSUPP.ps_suppkey
)
SELECT
  PART.p_name AS part_name,
  ROUND(COALESCE("_S11".SUM_REVENUE, 0), 2) AS revenue_generated
FROM TPCH.PARTSUPP PARTSUPP
JOIN "_T1" "_T1"
  ON PARTSUPP.ps_suppkey = "_T1".S_SUPPKEY
JOIN TPCH.PART PART
  ON PART.p_container LIKE 'LG%' AND PART.p_partkey = PARTSUPP.ps_partkey
LEFT JOIN "_S11" "_S11"
  ON PARTSUPP.ps_partkey = "_S11".PS_PARTKEY
  AND PARTSUPP.ps_suppkey = "_S11".PS_SUPPKEY
ORDER BY
  2 NULLS FIRST,
  1 NULLS FIRST
FETCH FIRST 8 ROWS ONLY
