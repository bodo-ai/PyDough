SELECT
  CONCAT_WS('', SUBSTRING(pr_brand, 1, 1), UPPER(SUBSTRING(pr_type, 1, 1))) AS combination,
  COUNT(*) AS n_products
FROM main.products
GROUP BY
  1
ORDER BY
  1
