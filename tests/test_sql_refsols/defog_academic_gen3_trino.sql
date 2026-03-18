SELECT
  year,
  COUNT(*) AS _expr0
FROM postgres.publication
GROUP BY
  1
