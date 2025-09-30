WITH _t2 AS (
  SELECT
    lineitem.l_extendedprice
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    part.p_brand = 'Brand#23' AND part.p_container = 'MED BOX'
  QUALIFY
    l_quantity < (
      0.2 * AVG(l_quantity) OVER (PARTITION BY l_partkey)
    )
)
SELECT
  COALESCE(SUM(l_extendedprice), 0) / 7.0 AS AVG_YEARLY
FROM _t2
