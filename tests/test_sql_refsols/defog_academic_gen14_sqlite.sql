SELECT
  year,
  COUNT(*) AS num_publications,
  COUNT(DISTINCT jid) AS num_journals,
  CAST(COUNT(*) AS REAL) / NULLIF(COUNT(DISTINCT jid), 0) AS ratio
FROM main.publication
GROUP BY
  1
