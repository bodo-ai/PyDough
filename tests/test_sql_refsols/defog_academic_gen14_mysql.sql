SELECT
  year,
  COUNT(*) AS num_publications,
  COUNT(DISTINCT jid) AS num_journals,
  COUNT(*) / NULLIF(COUNT(DISTINCT jid), 0) AS ratio
FROM publication
GROUP BY
  1
ORDER BY
  CASE WHEN year IS NULL THEN 1 ELSE 0 END,
  1
