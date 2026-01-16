WITH _s1 AS (
  SELECT
    column2 AS rule_category,
    column3 AS discount
  FROM (VALUES
    (1, 'A', 0.1),
    (2, 'B', 0.15),
    (3, 'C', 0.05)) AS pricing_rules(rule_id, rule_category, discount)
)
SELECT
  column2 AS product_category,
  AVG(column3) AS avg_price,
  COUNT(*) AS n_products,
  MIN(_s1.discount) AS min_discount
FROM (VALUES
  (1, 'A', 17.99),
  (2, 'B', 45.65),
  (3, 'A', 15.0),
  (4, 'B', 10.99)) AS products(product_id, product_category, price)
JOIN _s1 AS _s1
  ON _s1.rule_category = column2
GROUP BY
  1
