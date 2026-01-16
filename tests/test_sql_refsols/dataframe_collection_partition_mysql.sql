SELECT
  products.product_category,
  AVG(products.price) AS avg_price,
  COUNT(*) AS n_products,
  MIN(pricing_rules.discount) AS min_discount
FROM (VALUES
  ROW(1, 'A', 17.99),
  ROW(2, 'B', 45.65),
  ROW(3, 'A', 15.0),
  ROW(4, 'B', 10.99)) AS products(product_id, product_category, price)
JOIN (VALUES
  ROW(1, 'A', 0.1),
  ROW(2, 'B', 0.15),
  ROW(3, 'C', 0.05)) AS pricing_rules(rule_id, rule_category, discount)
  ON pricing_rules.rule_category = products.product_category
GROUP BY
  1
