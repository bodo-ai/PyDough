SELECT
  asian_nations_t7.nation_name,
  asian_custs_t7.ckey
FROM e2e_tests_db.public.asian_nations_t7 AS asian_nations_t7
JOIN e2e_tests_db.public.asian_custs_t7 AS asian_custs_t7
  ON asian_custs_t7.nkey = asian_nations_t7.nation_key
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
LIMIT 5
