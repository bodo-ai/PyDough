SELECT
  name
FROM e2e_tests_db.public.asian_nations_t2
WHERE
  CONTAINS(name, 'I')
