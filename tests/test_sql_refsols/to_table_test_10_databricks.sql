SELECT
  okey,
  odate
FROM e2e_tests_db.to_table_pyXXX.recent_orders_t10
WHERE
  odate < CAST('1995-06-01' AS DATE)
ORDER BY
  1 NULLS LAST
LIMIT 5
