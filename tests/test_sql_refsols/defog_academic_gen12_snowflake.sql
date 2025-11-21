SELECT
  COUNT_IF(NOT cid IS NULL) / NULLIF(COUNT_IF(NOT jid IS NULL), 0) AS ratio
FROM main.publication
