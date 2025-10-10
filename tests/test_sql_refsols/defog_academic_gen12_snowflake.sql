SELECT
  COUNT(DISTINCT cid) / COUNT(DISTINCT jid) AS ratio
FROM main.publication
