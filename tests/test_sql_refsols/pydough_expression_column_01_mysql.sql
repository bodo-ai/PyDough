SELECT
  CONCAT_WS('', SUBSTRING(pr_brand, 1, 1), UPPER(SUBSTRING(pr_type, 1, 1))) COLLATE utf8mb4_bin AS combination,
  COUNT(*) AS n_products
FROM main.PRODUCTS
GROUP BY
  1
ORDER BY
  1
