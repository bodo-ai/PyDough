SELECT
  CAST(SUM(IIF(NOT cid IS NULL, 1, 0)) AS REAL) / NULLIF(SUM(IIF(NOT jid IS NULL, 1, 0)), 0) AS ratio
FROM main.publication
