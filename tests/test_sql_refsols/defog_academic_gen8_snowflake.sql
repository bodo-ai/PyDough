SELECT
  title
FROM academic.publication
ORDER BY
  reference_num DESC NULLS LAST
LIMIT 3
