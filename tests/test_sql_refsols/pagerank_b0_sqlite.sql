WITH _s0 AS (
  SELECT
    COUNT(*) OVER () AS agg_2,
    s_key
  FROM main.sites
)
SELECT
  MAX(_s0.s_key) AS key,
  ROUND(CAST(1.0 AS REAL) / MAX(_s0.agg_2), 5) AS page_rank
FROM _s0 AS _s0
JOIN main.links AS links
  ON _s0.s_key = links.l_source
GROUP BY
  _s0.s_key
ORDER BY
  MAX(_s0.s_key)
