SELECT
  COUNT(*) AS TSC
FROM dealership.sales
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), sale_date) <= 7
