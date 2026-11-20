WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM mysql.tpch.part AS p
  CROSS JOIN UNNEST(SPLIT(p.p_name, ' ')) AS l(value)
), _s0 AS (
  SELECT
    color
  FROM _0
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _t AS (
  SELECT
    _s0.color,
    part.p_partkey,
    part.p_retailprice,
    ROW_NUMBER() OVER (
      PARTITION BY _s0.color
      ORDER BY part.p_retailprice DESC NULLS FIRST, part.p_partkey
    ) AS _w
  FROM _s0 AS _s0
  JOIN mysql.tpch.part AS part
    ON STARTS_WITH(
      part.p_name,
      CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', _s0.color, ' ') END
    )
    OR part.p_name LIKE CONCAT(
      '%',
      CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color, ' ') END,
      '%'
    )
    OR part.p_name LIKE CONCAT('%', CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color) END)
)
SELECT
  color,
  p_partkey AS key,
  p_retailprice AS retail_price
FROM _t
WHERE
  _w = 1
ORDER BY
  1 NULLS FIRST
