SELECT
  key,
  name
FROM e2e_tests_db.public.sorted_nations_t14
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
