SELECT
  CAST(SUM(NOT cid IS NULL) AS REAL) / NULLIF(SUM(NOT jid IS NULL), 0) AS ratio
FROM main.publication
