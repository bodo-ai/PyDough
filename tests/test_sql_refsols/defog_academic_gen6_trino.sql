SELECT
  title
FROM postgres.main.publication
ORDER BY
  citation_num DESC
LIMIT 1
