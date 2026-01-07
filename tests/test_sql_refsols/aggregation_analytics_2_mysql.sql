WITH _s6 AS (
  SELECT
    PARTSUPP.ps_partkey,
    SUM(
      LINEITEM.l_extendedprice * (
        1 - LINEITEM.l_discount
      ) * (
        1 - LINEITEM.l_tax
      ) - LINEITEM.l_quantity * PARTSUPP.ps_supplycost
    ) AS sum_revenue
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN tpch.SUPPLIER AS SUPPLIER
    ON PARTSUPP.ps_suppkey = SUPPLIER.s_suppkey
    AND SUPPLIER.s_name = 'Supplier#000000182'
  JOIN tpch.PART AS PART
    ON PART.p_container LIKE 'SM%' AND PART.p_partkey = PARTSUPP.ps_partkey
  JOIN tpch.LINEITEM AS LINEITEM
    ON EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) IN (1995, 1996)
    AND LINEITEM.l_partkey = PARTSUPP.ps_partkey
    AND LINEITEM.l_suppkey = PARTSUPP.ps_suppkey
  GROUP BY
    PARTSUPP.ps_suppkey,
    1
)
SELECT
  PART.p_name COLLATE utf8mb4_bin AS part_name,
  ROUND(COALESCE(_s6.sum_revenue, 0), 2) AS revenue_generated
FROM _s6 AS _s6
JOIN tpch.PART AS PART
  ON PART.p_partkey = _s6.ps_partkey
ORDER BY
  2,
  1
LIMIT 4
