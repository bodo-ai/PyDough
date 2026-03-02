SELECT
  COALESCE(SUM(item.quantity), 0) AS total_quatity
FROM item AS item
JOIN prod AS prod
  ON item.product_id = prod.product_id AND prod.category = 'Dresses'
WHERE
  item.channel_campaigns = 'Email'
  AND item.discounted
  AND item.sale_date = CAST('2025-06-04' AS DATE)
