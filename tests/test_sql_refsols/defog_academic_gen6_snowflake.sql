SELECT
  title
FROM main.publication
ORDER BY
  citation_num DESC NULLS LAST
LIMIT 1
