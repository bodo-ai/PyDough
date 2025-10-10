SELECT
  CAST(COUNT(DISTINCT cid) AS REAL) / COUNT(DISTINCT jid) AS ratio
FROM main.publication
