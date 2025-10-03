SELECT
  title
FROM main.publication
ORDER BY
  reference_num DESC NULLS LAST
LIMIT 3
