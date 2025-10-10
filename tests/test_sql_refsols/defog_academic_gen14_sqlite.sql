SELECT
  year,
  COUNT(DISTINCT pid) AS num_publications,
  COUNT(DISTINCT jid) AS num_journals,
  IIF(
    COUNT(DISTINCT jid) > 0,
    CAST(COUNT(DISTINCT pid) AS REAL) / COUNT(DISTINCT jid),
    NULL
  ) AS ratio
FROM main.publication
GROUP BY
  1
