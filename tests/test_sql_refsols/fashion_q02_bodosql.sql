WITH _s2 AS (
  SELECT
    prod.product_name,
    item.sale_id,
    COUNT(*) AS n_rows
  FROM prod AS prod
  JOIN item AS item
    ON item.product_id = prod.product_id
  WHERE
    CONTAINS(prod.product_name, 'Linen')
    AND prod.category = 'Sleepwear'
    AND prod.color = 'Green'
  GROUP BY
    1,
    2
), _s4 AS (
  SELECT
    sale.customer_id,
    _s2.product_name,
    SUM(_s2.n_rows) AS sum_n_rows
  FROM _s2 AS _s2
  JOIN sale AS sale
    ON _s2.sale_id = sale.sale_id
  GROUP BY
    1,
    2
)
SELECT
  _s4.product_name,
  cust.country,
  SUM(_s4.sum_n_rows) AS n_purchases
FROM _s4 AS _s4
JOIN cust AS cust
  ON _s4.customer_id = cust.customer_id
GROUP BY
  1,
  2
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
