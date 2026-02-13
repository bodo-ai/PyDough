WITH _t AS (
  SELECT
    LINEITEM.l_extendedprice,
    LINEITEM.l_quantity,
    AVG(CAST(LINEITEM.l_quantity AS DOUBLE)) OVER (PARTITION BY LINEITEM.l_partkey) AS _w
  FROM tpch.PART AS PART
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_partkey = PART.p_partkey
  WHERE
    PART.p_brand = 'Brand#23' AND PART.p_container = 'MED BOX'
)
SELECT
  COALESCE(SUM(l_extendedprice), 0) / 7.0 AS AVG_YEARLY
FROM _t
WHERE
  l_quantity < (
    0.2 * _w
  )
