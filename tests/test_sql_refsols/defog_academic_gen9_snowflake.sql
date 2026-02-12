SELECT
  title,
  citation_num
FROM academic.publication
ORDER BY
  2 DESC NULLS LAST
LIMIT 3
