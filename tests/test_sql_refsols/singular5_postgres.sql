WITH _t AS (
  SELECT
    lineitem.l_shipdate,
    part.p_container,
    ROW_NUMBER() OVER (PARTITION BY part.p_container ORDER BY lineitem.l_extendedprice DESC, lineitem.l_shipdate) AS _w
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
    AND lineitem.l_shipmode = 'RAIL'
    AND lineitem.l_tax = 0
  WHERE
    part.p_brand = 'Brand#13'
)
SELECT
  part.p_container AS container,
  MAX(_t.l_shipdate) AS highest_price_ship_date
FROM tpch.part AS part
JOIN _t AS _t
  ON _t._w = 1 AND _t.p_container = part.p_container
WHERE
  part.p_brand = 'Brand#13'
GROUP BY
  1
ORDER BY
  2 NULLS FIRST,
  1 NULLS FIRST
LIMIT 5
