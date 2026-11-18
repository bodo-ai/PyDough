WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM tpch.PART AS p
  CROSS JOIN JSON_TABLE(CONCAT('["', REPLACE(p.p_name, ' ', '","'), '"]'), '$[*]' COLUMNS(value VARCHAR(255) PATH '$')) AS l
), _s0 AS (
  SELECT
    color COLLATE utf8mb4_bin AS color
  FROM _0
  ORDER BY
    1
  LIMIT 5
), _t AS (
  SELECT
    _s0.color,
    PART.p_partkey,
    PART.p_retailprice,
    ROW_NUMBER() OVER (
      PARTITION BY _s0.color
      ORDER BY CASE WHEN PART.p_retailprice IS NULL THEN 1 ELSE 0 END DESC, PART.p_retailprice DESC, CASE WHEN PART.p_partkey IS NULL THEN 1 ELSE 0 END, PART.p_partkey
    ) AS _w
  FROM _s0 AS _s0
  JOIN tpch.PART AS PART
    ON PART.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _s0.color, ' '), '%')
    OR PART.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _s0.color))
    OR PART.p_name LIKE CONCAT(CONCAT_WS('', _s0.color, ' '), '%')
)
SELECT
  color COLLATE utf8mb4_bin AS color,
  p_partkey AS `key`,
  p_retailprice AS retail_price
FROM _t
WHERE
  _w = 1
ORDER BY
  1
