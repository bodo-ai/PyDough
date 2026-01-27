SELECT
  year,
  COUNT(*) AS num_publications,
  COUNT(DISTINCT jid) AS num_journals,
  COUNT(*) / NULLIF(COUNT(DISTINCT jid), 0) AS ratio
FROM academic.publication
GROUP BY
  1
