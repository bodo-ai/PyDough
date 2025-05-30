WITH _t AS (
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
  container,
  ship_date AS highest_price_ship_date
FROM _t
WHERE
  _w = 1
ORDER BY
  highest_price_ship_date,
  container
LIMIT 5
