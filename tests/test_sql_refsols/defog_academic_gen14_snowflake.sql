SELECT
  year,
  COUNT(*) AS num_publications,
  COUNT(DISTINCT jid) AS num_journals,
  COUNT(*) / CASE WHEN COUNT(DISTINCT jid) > 0 THEN COUNT(DISTINCT jid) ELSE NULL END AS ratio
FROM main.publication
GROUP BY
  1
