WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN JSON_EACH('["' || REPLACE(p.p_name, ' ', '","') || '"]') AS l
), _s0 AS (
  SELECT
    color
  FROM _0
  ORDER BY
    1
  LIMIT 5
), _t AS (
  SELECT
    _s0.color,
    part.p_partkey,
    part.p_retailprice,
    ROW_NUMBER() OVER (PARTITION BY _s0.color ORDER BY part.p_retailprice DESC, part.p_partkey) AS _w
  FROM _s0 AS _s0
  JOIN tpch.part AS part
    ON part.p_name LIKE (
      '%' || CONCAT_WS('', ' ', _s0.color)
    )
    OR part.p_name LIKE (
      CONCAT_WS('', _s0.color, ' ') || '%'
    )
    OR part.p_name LIKE (
      '%' || CONCAT_WS('', ' ', _s0.color, ' ') || '%'
    )
)
SELECT
  color,
  p_partkey AS key,
  p_retailprice AS retail_price
FROM _t
WHERE
  _w = 1
ORDER BY
  1
