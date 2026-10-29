SELECT
  pr_id AS _id,
  pr_name AS name,
  pr_type AS category,
  pr_brand AS brand,
  pr_prodcost AS production_cost,
  pr_release AS release_date,
  CONCAT_WS('', SUBSTRING(pr_brand, 1, 1), UPPER(SUBSTRING(pr_type, 1, 1))) AS brand_cat
FROM main.products
ORDER BY
  1 NULLS FIRST
LIMIT 3
