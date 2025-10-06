SELECT
  year,
  COUNT(DISTINCT pid) AS num_publications,
  COUNT(DISTINCT jid) AS num_journals,
  IFF(COUNT(DISTINCT jid) > 0, COUNT(DISTINCT pid) / COUNT(DISTINCT jid), NULL) AS ratio
FROM main.publication
GROUP BY
  1
