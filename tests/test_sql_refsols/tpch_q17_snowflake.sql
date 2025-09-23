WITH _t2 AS (
  SELECT
    l_extendedprice
  FROM tpch.lineitem
  QUALIFY
    l_quantity < (
      0.2 * AVG(l_quantity) OVER (PARTITION BY l_partkey)
    )
)
SELECT
  COALESCE(SUM(l_extendedprice), 0) / 7.0 AS AVG_YEARLY
FROM _t2
