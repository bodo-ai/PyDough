WITH _s1 AS (
  SELECT
    de_product_id,
    COUNT(*) AS n_rows
  FROM main.devices
  GROUP BY
    1
)
SELECT
  CONCAT_WS('', SUBSTRING(products.pr_brand, 1, 1), UPPER(SUBSTRING(products.pr_type, 1, 1))) AS combination,
  COUNT(*) AS n_products,
  SUM(_s1.n_rows) AS n_devices
FROM main.products AS products
JOIN _s1 AS _s1
  ON _s1.de_product_id = products.pr_id
GROUP BY
  1
ORDER BY
  1
