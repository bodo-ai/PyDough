SELECT
  pr_id AS _id,
  CASE
    WHEN SUBSTRING(pr_brand, 1, 1) IS NULL OR UPPER(SUBSTRING(pr_type, 1, 1)) IS NULL
    THEN NULL
    ELSE CONCAT_WS('', SUBSTRING(pr_brand, 1, 1), UPPER(SUBSTRING(pr_type, 1, 1)))
  END AS brand_cat
FROM main.products
ORDER BY
  1
LIMIT 3
