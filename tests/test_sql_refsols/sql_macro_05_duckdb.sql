WITH _s0 AS (
  SELECT
    p_name,
    p_partkey
  FROM main.part
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
), _0 AS (
  SELECT DISTINCT
    UNNEST(STR_SPLIT(p_name, ' ')) AS color
  FROM part
)
SELECT
  ANY_VALUE(_s0.p_name) AS name,
  MIN(_0.color) AS a,
  MAX(_0.color) AS b
FROM _s0 AS _s0
JOIN _0 AS _0
  ON _s0.p_name LIKE (
    '%' || CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color) END
  )
  OR _s0.p_name LIKE (
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', _0.color, ' ') END || '%'
  )
  OR _s0.p_name LIKE (
    '%' || CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color, ' ') END || '%'
  )
GROUP BY
  _s0.p_partkey
ORDER BY
  _s0.p_partkey NULLS FIRST
