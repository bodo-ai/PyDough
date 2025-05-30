WITH _s2 AS (
  SELECT DISTINCT
    p_container AS container
  FROM tpch.part
  WHERE
    p_brand = 'Brand#13'
), _t AS (
  SELECT
    part.p_container AS container,
    lineitem.l_shipdate AS ship_date,
    ROW_NUMBER() OVER (PARTITION BY part.p_container ORDER BY lineitem.l_extendedprice DESC, lineitem.l_shipdate) AS _w
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey AND lineitem.l_shipmode = 'RAIL'
  WHERE
    part.p_brand = 'Brand#13'
)
SELECT
  _s2.container,
  _t.ship_date AS highest_price_ship_date
FROM _s2 AS _s2
JOIN _t AS _t
  ON _s2.container = _t.container AND _t._w = 1
ORDER BY
  highest_price_ship_date,
  container
LIMIT 5
