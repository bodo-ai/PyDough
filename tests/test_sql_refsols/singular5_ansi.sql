WITH _s2 AS (
  SELECT DISTINCT
    p_container AS container
  FROM tpch.part
  WHERE
    p_brand = 'Brand#13'
), _t2 AS (
  SELECT
    part.p_container AS container,
    lineitem.l_shipdate AS ship_date
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey AND lineitem.l_shipmode = 'RAIL'
  WHERE
    part.p_brand = 'Brand#13'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY part.p_container ORDER BY lineitem.l_extendedprice DESC NULLS FIRST, lineitem.l_shipdate NULLS LAST) = 1
)
SELECT
  _s2.container,
  _t2.ship_date AS highest_price_ship_date
FROM _s2 AS _s2
JOIN _t2 AS _t2
  ON _s2.container = _t2.container
ORDER BY
  highest_price_ship_date,
  container
LIMIT 5
