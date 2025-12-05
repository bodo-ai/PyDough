WITH _s6 AS (
  SELECT
    partsupp.ps_partkey,
    SUM(
      lineitem.l_extendedprice * (
        1 - lineitem.l_discount
      ) * (
        1 - lineitem.l_tax
      ) - lineitem.l_quantity * partsupp.ps_supplycost
    ) AS sum_revenue
  FROM tpch.partsupp AS partsupp
  JOIN tpch.supplier AS supplier
    ON partsupp.ps_suppkey = supplier.s_suppkey
    AND supplier.s_name = 'Supplier#000000182'
  JOIN tpch.part AS part
    ON part.p_container LIKE 'SM%' AND part.p_partkey = partsupp.ps_partkey
  JOIN tpch.lineitem AS lineitem
    ON EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) IN (1995, 1996)
    AND lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  GROUP BY
    partsupp.ps_suppkey,
    1
)
SELECT
  part.p_name AS part_name,
  ROUND(COALESCE(_s6.sum_revenue, 0), 2) AS revenue_generated
FROM _s6 AS _s6
JOIN tpch.part AS part
  ON _s6.ps_partkey = part.p_partkey
ORDER BY
  2,
  1
LIMIT 4
