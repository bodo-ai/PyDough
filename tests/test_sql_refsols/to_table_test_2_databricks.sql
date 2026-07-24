SELECT
  name
FROM e2e_tests_db.to_table_pyXXX.asian_nations_t2
WHERE
  CONTAINS(name, 'I')
