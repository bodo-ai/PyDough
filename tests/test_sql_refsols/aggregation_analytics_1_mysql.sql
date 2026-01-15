WITH _t1 AS (
  SELECT
    s_name,
    s_suppkey
  FROM tpch.SUPPLIER
  WHERE
    s_name = 'Supplier#000009450'
), _s11 AS (
  SELECT
    PARTSUPP.ps_partkey,
    PARTSUPP.ps_suppkey,
    SUM(
      LINEITEM.l_extendedprice * (
        1 - LINEITEM.l_discount
      ) * (
        1 - LINEITEM.l_tax
      ) - LINEITEM.l_quantity * PARTSUPP.ps_supplycost
    ) AS sum_revenue
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN _t1 AS _t4
    ON PARTSUPP.ps_suppkey = _t4.s_suppkey
  JOIN tpch.PART AS PART
    ON PART.p_container LIKE 'LG%' AND PART.p_partkey = PARTSUPP.ps_partkey
  JOIN tpch.LINEITEM AS LINEITEM
    ON EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) IN (1995, 1996)
    AND LINEITEM.l_partkey = PARTSUPP.ps_partkey
    AND LINEITEM.l_suppkey = PARTSUPP.ps_suppkey
  GROUP BY
    1,
    2
)
SELECT
  PART.p_name COLLATE utf8mb4_bin AS part_name,
  ROUND(COALESCE(_s11.sum_revenue, 0), 2) AS revenue_generated
FROM tpch.PARTSUPP AS PARTSUPP
JOIN _t1 AS _t1
  ON PARTSUPP.ps_suppkey = _t1.s_suppkey
JOIN tpch.PART AS PART
  ON PART.p_container LIKE 'LG%' AND PART.p_partkey = PARTSUPP.ps_partkey
LEFT JOIN _s11 AS _s11
  ON PARTSUPP.ps_partkey = _s11.ps_partkey AND PARTSUPP.ps_suppkey = _s11.ps_suppkey
ORDER BY
  2,
  1
LIMIT 8
