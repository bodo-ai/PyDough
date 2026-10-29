SELECT
  pr_id AS _id,
  CASE
    WHEN SUBSTR(pr_brand, 1, 1) IS NULL OR UPPER(SUBSTR(pr_type, 1, 1)) IS NULL
    THEN NULL
    ELSE CONCAT_WS('', SUBSTR(pr_brand, 1, 1), UPPER(SUBSTR(pr_type, 1, 1)))
  END AS brand_cat
FROM main.products
ORDER BY
  1 NULLS FIRST
LIMIT 3
