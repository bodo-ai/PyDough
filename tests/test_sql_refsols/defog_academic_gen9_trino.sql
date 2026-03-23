SELECT
  title,
  citation_num
FROM postgres.main.publication
ORDER BY
  2 DESC
LIMIT 3
