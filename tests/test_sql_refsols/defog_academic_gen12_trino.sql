SELECT
  CAST(SUM(NOT cid IS NULL) AS DOUBLE) / NULLIF(SUM(NOT jid IS NULL), 0) AS ratio
FROM main.publication
