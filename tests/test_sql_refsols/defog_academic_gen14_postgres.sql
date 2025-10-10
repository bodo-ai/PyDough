SELECT
  year,
  COUNT(DISTINCT pid) AS num_publications,
  COUNT(DISTINCT jid) AS num_journals,
  CASE
    WHEN COUNT(DISTINCT jid) > 0
    THEN CAST(COUNT(DISTINCT pid) AS DOUBLE PRECISION) / COUNT(DISTINCT jid)
    ELSE NULL
  END AS ratio
FROM main.publication
GROUP BY
  1
