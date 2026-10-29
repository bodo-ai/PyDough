SELECT
  PR_ID AS "_id",
  PR_NAME AS name,
  PR_TYPE AS category,
  PR_BRAND AS brand,
  PR_PRODCOST AS production_cost,
  PR_RELEASE AS release_date,
  NVL(SUBSTR(PR_BRAND, 1, 1), '') || '' || NVL(UPPER(SUBSTR(PR_TYPE, 1, 1)), '') AS brand_cat
FROM MAIN.PRODUCTS
ORDER BY
  1 NULLS FIRST
FETCH FIRST 3 ROWS ONLY
