WITH _t1 AS (
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
  container,
  ship_date AS highest_price_ship_date
FROM _t1
ORDER BY
  highest_price_ship_date,
  container
LIMIT 5
