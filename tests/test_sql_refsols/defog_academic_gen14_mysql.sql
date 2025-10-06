SELECT
  year,
  COUNT(DISTINCT pid) AS num_publications,
  COUNT(DISTINCT jid) AS num_journals,
  CASE
    WHEN COUNT(DISTINCT jid) > 0
    THEN COUNT(DISTINCT pid) / COUNT(DISTINCT jid)
    ELSE NULL
  END AS ratio
FROM main.publication
GROUP BY
  1
