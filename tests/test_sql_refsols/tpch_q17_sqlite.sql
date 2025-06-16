WITH _t AS (
  SELECT
    lineitem.l_extendedprice,
    lineitem.l_quantity,
    AVG(lineitem.l_quantity) OVER (PARTITION BY lineitem.l_partkey) AS _w
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    part.p_brand = 'Brand#23' AND part.p_container = 'MED BOX'
), _t0 AS (
  SELECT
    SUM(l_extendedprice) AS agg_0
  FROM _t
  WHERE
    l_quantity < (
      0.2 * _w
    )
)
SELECT
  CAST(COALESCE(agg_0, 0) AS REAL) / 7.0 AS AVG_YEARLY
FROM _t0
