SELECT
  COALESCE(SUM(item.quantity), 0) AS total_quatity
FROM item AS item
JOIN prod AS prod
  ON item.product_id = prod.product_id AND prod.color = 'White' AND prod.size = 'M'
