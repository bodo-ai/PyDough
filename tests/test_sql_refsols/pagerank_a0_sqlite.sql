SELECT
  s_key AS key,
  ROUND(CAST(1.0 AS REAL) / COUNT(*) OVER (), 5) AS page_rank
FROM main.sites
ORDER BY
  1
