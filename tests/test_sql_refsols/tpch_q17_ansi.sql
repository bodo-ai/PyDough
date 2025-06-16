WITH _t2 AS (
  SELECT
    lineitem.l_extendedprice
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    part.p_brand = 'Brand#23' AND part.p_container = 'MED BOX'
  QUALIFY
    lineitem.l_quantity < (
      0.2 * AVG(lineitem.l_quantity) OVER (PARTITION BY lineitem.l_partkey)
    )
), _t0 AS (
  SELECT
    SUM(l_extendedprice) AS agg_0
  FROM _t2
)
SELECT
  COALESCE(agg_0, 0) / 7.0 AS AVG_YEARLY
FROM _t0
