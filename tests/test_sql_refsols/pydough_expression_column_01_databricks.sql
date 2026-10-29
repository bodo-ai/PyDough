SELECT
  CASE
    WHEN '' IS NULL
    OR SUBSTRING(pr_brand, 1, 1) IS NULL
    OR UPPER(SUBSTRING(pr_type, 1, 1)) IS NULL
    THEN NULL
    ELSE CONCAT_WS('', SUBSTRING(pr_brand, 1, 1), UPPER(SUBSTRING(pr_type, 1, 1)))
  END AS combination,
  COUNT(*) AS n_products
FROM main.products
GROUP BY
  1
ORDER BY
  CASE
    WHEN SUBSTRING(pr_brand, 1, 1) IS NULL OR UPPER(SUBSTRING(pr_type, 1, 1)) IS NULL
    THEN NULL
    ELSE CONCAT_WS('', SUBSTRING(pr_brand, 1, 1), UPPER(SUBSTRING(pr_type, 1, 1)))
  END
