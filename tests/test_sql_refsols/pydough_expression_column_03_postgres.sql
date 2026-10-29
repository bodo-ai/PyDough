SELECT
  pr_id AS _id,
  pr_name AS name,
  pr_type AS category,
  pr_brand AS brand,
  pr_prodcost AS production_cost,
  pr_release AS release_date,
  CASE
    WHEN SUBSTRING(pr_brand FROM 1 FOR 1) IS NULL
    OR UPPER(SUBSTRING(pr_type FROM 1 FOR 1)) IS NULL
    THEN NULL
    ELSE CONCAT_WS('', SUBSTRING(pr_brand FROM 1 FOR 1), UPPER(SUBSTRING(pr_type FROM 1 FOR 1)))
  END AS brand_cat
FROM main.products
ORDER BY
  1 NULLS FIRST
LIMIT 3
