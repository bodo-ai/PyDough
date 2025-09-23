WITH _t AS (
  SELECT
    l_extendedprice,
    l_quantity,
    AVG(l_quantity) OVER (PARTITION BY l_partkey) AS _w
  FROM tpch.lineitem
)
SELECT
  CAST(COALESCE(SUM(l_extendedprice), 0) AS REAL) / 7.0 AS AVG_YEARLY
FROM _t
WHERE
  l_quantity < (
    0.2 * _w
  )
