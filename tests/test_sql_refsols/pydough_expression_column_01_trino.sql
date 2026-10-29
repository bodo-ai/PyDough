SELECT
  CASE
    WHEN '' IS NULL
    OR SUBSTR(pr_brand, 1, 1) IS NULL
    OR UPPER(SUBSTR(pr_type, 1, 1)) IS NULL
    THEN NULL
    ELSE CONCAT_WS('', SUBSTR(pr_brand, 1, 1), UPPER(SUBSTR(pr_type, 1, 1)))
  END AS combination,
  COUNT(*) AS n_products
FROM main.products
GROUP BY
  1
ORDER BY
  CASE
    WHEN SUBSTR(pr_brand, 1, 1) IS NULL OR UPPER(SUBSTR(pr_type, 1, 1)) IS NULL
    THEN NULL
    ELSE CONCAT_WS('', SUBSTR(pr_brand, 1, 1), UPPER(SUBSTR(pr_type, 1, 1)))
  END NULLS FIRST
