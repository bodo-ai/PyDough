WITH _t0 AS (
  SELECT
    ROUND(CAST(1.0 AS REAL) / COUNT(*) OVER (), 5) AS page_rank_0,
    s_key
  FROM main.sites
)
SELECT
  s_key AS key,
  page_rank_0 AS page_rank
FROM _t0
ORDER BY
  s_key
