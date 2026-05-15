SELECT
  products_collection.product_category,
  AVG(products_collection.price) AS avg_price,
  COUNT(*) AS n_products,
  MIN(pricing_collection.discount) AS min_discount
FROM (VALUES
  ROW(1, 'A', 17.99),
  ROW(2, 'B', 45.65),
  ROW(3, 'A', 15.0),
  ROW(4, 'B', 10.99)) AS products_collection(product_id, product_category, price)
JOIN (VALUES
  ROW(1, 'A', 0.1),
  ROW(2, 'B', 0.15),
  ROW(3, 'C', 0.05)) AS pricing_collection(rule_id, rule_category, discount)
  ON pricing_collection.rule_category = products_collection.product_category
GROUP BY
  1
