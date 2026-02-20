WITH _s5 AS (
  SELECT
    item.product_id,
    SUM(item.quantity) AS sum_quantity
  FROM item AS item
  JOIN sale AS sale
    ON item.sale_id = sale.sale_id
  JOIN cust AS cust
    ON YEAR(CAST(cust.signup_date AS TIMESTAMP)) = 2024
    AND cust.age_range = '16-25'
    AND cust.country = 'Spain'
    AND cust.customer_id = sale.customer_id
  GROUP BY
    1
)
SELECT
  prod.product_name,
  COALESCE(_s5.sum_quantity, 0) AS qty_purchased
FROM prod AS prod
JOIN _s5 AS _s5
  ON _s5.product_id = prod.product_id
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 3
