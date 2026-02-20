WITH _t0 AS (
  SELECT
    stok.country,
    prod.product_name,
    stok.stock_quantity
  FROM prod AS prod
  JOIN stok AS stok
    ON prod.product_id = stok.product_id
  WHERE
    CONTAINS(prod.product_name, 'Silk')
    AND prod.category = 'Dresses'
    AND prod.color = 'Red'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY stok.product_id ORDER BY stok.stock_quantity DESC) = 1
)
SELECT
  product_name,
  country,
  stock_quantity AS quantity
FROM _t0
ORDER BY
  1 NULLS FIRST
