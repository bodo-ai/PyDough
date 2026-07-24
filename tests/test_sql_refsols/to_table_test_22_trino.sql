SELECT
  mult_t22.mult,
  prod_t22.pid,
  prod_t22.product_name AS pname
FROM memory.default.mult_t22 AS mult_t22
CROSS JOIN memory.default.prod_t22 AS prod_t22
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
