SELECT
  pr_id AS _id,
  CONCAT_WS('', SUBSTRING(pr_brand, 1, 1), UPPER(SUBSTRING(pr_type, 1, 1))) AS brand_cat
FROM main.products
ORDER BY
  1
LIMIT 3
