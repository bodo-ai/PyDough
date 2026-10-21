WITH _t AS (
  SELECT
    LINEITEM.l_shipdate,
    PART.p_container,
    ROW_NUMBER() OVER (PARTITION BY PART.p_container ORDER BY CASE WHEN LINEITEM.l_extendedprice IS NULL THEN 1 ELSE 0 END DESC, LINEITEM.l_extendedprice DESC, CASE WHEN LINEITEM.l_shipdate IS NULL THEN 1 ELSE 0 END, LINEITEM.l_shipdate) AS _w
  FROM tpch.PART AS PART
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_partkey = PART.p_partkey
    AND LINEITEM.l_shipmode = 'RAIL'
    AND LINEITEM.l_tax = 0
  WHERE
    PART.p_brand = 'Brand#13'
)
SELECT
  PART.p_container COLLATE utf8mb4_bin AS container,
  ANY_VALUE(_t.l_shipdate) AS highest_price_ship_date
FROM tpch.PART AS PART
JOIN _t AS _t
  ON PART.p_container = _t.p_container AND _t._w = 1
WHERE
  PART.p_brand = 'Brand#13'
GROUP BY
  1
ORDER BY
  2,
  1
LIMIT 5
