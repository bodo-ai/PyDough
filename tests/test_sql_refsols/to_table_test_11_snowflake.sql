SELECT
  pkey,
  pname,
  psize
FROM e2e_tests_db.public.parts_summary_t11
ORDER BY
  3 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
