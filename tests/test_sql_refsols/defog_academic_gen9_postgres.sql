SELECT
  title,
  citation_num
FROM main.publication
ORDER BY
  2 DESC NULLS LAST
LIMIT 3
