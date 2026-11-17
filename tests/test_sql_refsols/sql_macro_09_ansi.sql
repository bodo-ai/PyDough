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
), _t0 AS (
  SELECT
    _s0.color,
    part.p_partkey,
    part.p_retailprice
  FROM _s0 AS _s0
  JOIN tpch.part AS part
    ON part.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _s0.color, ' '), '%')
    OR part.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _s0.color))
    OR part.p_name LIKE CONCAT(CONCAT_WS('', _s0.color, ' '), '%')
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY _s0.color
      ORDER BY part.p_retailprice DESC NULLS FIRST, part.p_partkey NULLS LAST
    ) = 1
)
SELECT
  color,
  p_partkey AS key,
  p_retailprice AS retail_price
FROM _t0
