WITH _s7 AS (
  SELECT
    lineitem.l_quantity,
    lineitem.l_suppkey
  FROM tpch.lineitem AS lineitem
  JOIN tpch.part AS part
    ON lineitem.l_partkey = part.p_partkey
    AND part.p_container LIKE 'LG%'
    AND part.p_name LIKE '%tomato%'
  WHERE
    EXTRACT(MONTH FROM CAST(lineitem.l_shipdate AS TIMESTAMP)) < 7
    AND EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS TIMESTAMP)) = 1995
    AND lineitem.l_shipmode = 'SHIP'
), _t0 AS (
  SELECT
    MAX(nation.n_name) AS anything_n_name,
    MAX(supplier.s_name) AS anything_s_name,
    MAX(supplier.s_nationkey) AS anything_s_nationkey,
    SUM(_s7.l_quantity) AS sum_l_quantity
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AFRICA'
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
    AND supplier.s_acctbal >= 8000.0
    AND supplier.s_comment LIKE '%careful%'
  LEFT JOIN _s7 AS _s7
    ON _s7.l_suppkey = supplier.s_suppkey
  GROUP BY
    supplier.s_suppkey
)
SELECT
  anything_s_name AS supplier_name,
  anything_n_name AS nation_name,
  COALESCE(sum_l_quantity, 0) AS supplier_quantity,
  (
    100.0 * COALESCE(sum_l_quantity, 0)
  ) / CASE
    WHEN SUM(COALESCE(sum_l_quantity, 0)) OVER (PARTITION BY anything_s_nationkey) > 0
    THEN SUM(COALESCE(sum_l_quantity, 0)) OVER (PARTITION BY anything_s_nationkey)
    ELSE NULL
  END AS national_qty_pct
FROM _t0
ORDER BY
  4 DESC NULLS LAST
LIMIT 5
