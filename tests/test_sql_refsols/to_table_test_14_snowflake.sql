SELECT
  key,
  name
FROM defog.public.sorted_nations_t14
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
