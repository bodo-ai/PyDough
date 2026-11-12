WITH _s0 AS (
  SELECT
    p_name,
    p_partkey
  FROM tpch_s3_pq.tpch_sf1.part
  ORDER BY
    2
  LIMIT 3
), _0 AS (
  SELECT
    EXPLODE(SPLIT(p_name, ' ')) AS color
  FROM part
)
SELECT
  ANY_VALUE(_s0.p_name) AS name,
  MIN(_0.color) AS a,
  MAX(_0.color) AS b
FROM _s0 AS _s0
JOIN _0 AS _0
  ON CONTAINS(
    _s0.p_name,
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color, ' ') END
  )
  OR ENDSWITH(
    _s0.p_name,
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color) END
  )
  OR STARTSWITH(
    _s0.p_name,
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', _0.color, ' ') END
  )
GROUP BY
  _s0.p_partkey
ORDER BY
  _s0.p_partkey
