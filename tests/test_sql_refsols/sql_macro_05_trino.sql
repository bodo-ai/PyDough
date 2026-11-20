WITH _s0 AS (
  SELECT
    p_name,
    p_partkey
  FROM mysql.tpch.part
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
), _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM mysql.tpch.part AS p
  CROSS JOIN UNNEST(SPLIT(p.p_name, ' ')) AS l(value)
)
SELECT
  ARBITRARY(_s0.p_name) AS name,
  MIN(_0.color) AS a,
  MAX(_0.color) AS b
FROM _s0 AS _s0
JOIN _0 AS _0
  ON STARTS_WITH(
    _s0.p_name,
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', _0.color, ' ') END
  )
  OR _s0.p_name LIKE CONCAT(
    '%',
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color, ' ') END,
    '%'
  )
  OR _s0.p_name LIKE CONCAT('%', CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color) END)
GROUP BY
  _s0.p_partkey
ORDER BY
  _s0.p_partkey NULLS FIRST
