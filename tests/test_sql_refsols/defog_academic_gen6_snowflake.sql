SELECT
  title
FROM academic.publication
ORDER BY
  citation_num DESC NULLS LAST
LIMIT 1
