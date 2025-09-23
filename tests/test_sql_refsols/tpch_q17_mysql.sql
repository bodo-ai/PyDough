WITH _t AS (
  SELECT
    l_extendedprice,
    l_quantity,
    AVG(l_quantity) OVER (PARTITION BY l_partkey) AS _w
  FROM tpch.LINEITEM
)
SELECT
  COALESCE(SUM(l_extendedprice), 0) / 7.0 AS AVG_YEARLY
FROM _t
WHERE
  l_quantity < (
    0.2 * _w
  )
