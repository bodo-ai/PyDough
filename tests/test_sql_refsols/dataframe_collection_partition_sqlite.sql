SELECT
  products_collection.column2 AS product_category,
  AVG(products_collection.column3) AS avg_price,
  COUNT(*) AS n_products,
  MIN(pricing_collection.column3) AS min_discount
FROM (VALUES
  (1, 'A', 17.99),
  (2, 'B', 45.65),
  (3, 'A', 15.0),
  (4, 'B', 10.99)) AS products_collection
JOIN (VALUES
  (1, 'A', 0.1),
  (2, 'B', 0.15),
  (3, 'C', 0.05)) AS pricing_collection
  ON pricing_collection.column2 = products_collection.column2
GROUP BY
  1
