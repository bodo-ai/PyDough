WITH _t3 AS (
  SELECT
    lineitem.l_shipdate,
    part.p_container
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
    AND lineitem.l_shipmode = 'RAIL'
    AND lineitem.l_tax = 0
  WHERE
    part.p_brand = 'Brand#13'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY part.p_container ORDER BY lineitem.l_extendedprice DESC NULLS FIRST, lineitem.l_shipdate) = 1
)
SELECT
  part.p_container AS container,
  ANY_VALUE(_t3.l_shipdate) AS highest_price_ship_date
FROM tpch.part AS part
JOIN _t3 AS _t3
  ON _t3.p_container = part.p_container
WHERE
  part.p_brand = 'Brand#13'
GROUP BY
  1
ORDER BY
  2 NULLS FIRST,
  1 NULLS FIRST
LIMIT 5
