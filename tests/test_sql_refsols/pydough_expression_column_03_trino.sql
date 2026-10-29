SELECT
  pr_id AS _id,
  pr_name AS name,
  pr_type AS category,
  pr_brand AS brand,
  pr_prodcost AS production_cost,
  pr_release AS release_date,
  CASE
    WHEN SUBSTR(pr_brand, 1, 1) IS NULL OR UPPER(SUBSTR(pr_type, 1, 1)) IS NULL
    THEN NULL
    ELSE CONCAT_WS('', SUBSTR(pr_brand, 1, 1), UPPER(SUBSTR(pr_type, 1, 1)))
  END AS brand_cat
FROM main.products
ORDER BY
  1 NULLS FIRST
LIMIT 3
