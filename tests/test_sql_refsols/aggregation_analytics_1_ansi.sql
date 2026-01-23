WITH _t1 AS (
  SELECT
    s_name,
    s_suppkey
  FROM tpch.supplier
  WHERE
    s_name = 'Supplier#000009450'
), _s11 AS (
  SELECT
    partsupp.ps_partkey,
    partsupp.ps_suppkey,
    SUM(
      lineitem.l_extendedprice * (
        1 - lineitem.l_discount
      ) * (
        1 - lineitem.l_tax
      ) - lineitem.l_quantity * partsupp.ps_supplycost
    ) AS sum_revenue
  FROM tpch.partsupp AS partsupp
  JOIN _t1 AS _t4
    ON _t4.s_suppkey = partsupp.ps_suppkey
  JOIN tpch.part AS part
    ON part.p_container LIKE 'LG%' AND part.p_partkey = partsupp.ps_partkey
  JOIN tpch.lineitem AS lineitem
    ON EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) IN (1995, 1996)
    AND lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  GROUP BY
    1,
    2
)
SELECT
  part.p_name AS part_name,
  ROUND(COALESCE(_s11.sum_revenue, 0), 2) AS revenue_generated
FROM tpch.partsupp AS partsupp
JOIN _t1 AS _t1
  ON _t1.s_suppkey = partsupp.ps_suppkey
JOIN tpch.part AS part
  ON part.p_container LIKE 'LG%' AND part.p_partkey = partsupp.ps_partkey
LEFT JOIN _s11 AS _s11
  ON _s11.ps_partkey = partsupp.ps_partkey AND _s11.ps_suppkey = partsupp.ps_suppkey
ORDER BY
  2,
  1
LIMIT 8
