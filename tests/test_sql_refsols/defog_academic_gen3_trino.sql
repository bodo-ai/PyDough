SELECT
  year,
  COUNT(*) AS _expr0
FROM postgres.main.publication
GROUP BY
  1
