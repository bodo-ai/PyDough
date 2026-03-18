SELECT
  title,
  citation_num
FROM postgres.publication
ORDER BY
  2 DESC
LIMIT 3
