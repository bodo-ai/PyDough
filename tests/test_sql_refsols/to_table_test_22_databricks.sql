SELECT
  mult_t22.mult,
  prod_t22.pid,
  prod_t22.product_name AS pname
FROM e2e_tests_db.to_table_pyXXX.mult_t22 AS mult_t22
CROSS JOIN e2e_tests_db.to_table_pyXXX.prod_t22 AS prod_t22
ORDER BY
  1,
  2
