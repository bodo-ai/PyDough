SELECT
  sbCustState COLLATE utf8mb4_bin AS state,
  COUNT(DISTINCT sbCustCity) AS a1,
  COUNT(*) AS a2,
  COUNT(CASE WHEN LOWER(sbCustName) LIKE 'j%' THEN sbCustName ELSE NULL END) AS a3,
  COALESCE(SUM(TRUNCATE(CAST(sbCustPostalCode AS FLOAT), 0)), 0) AS a4,
  MIN(sbCustPhone) AS a5,
  MAX(sbCustPhone) AS a6,
  ANY_VALUE(LOWER(sbCustState)) AS a7,
  ANY_VALUE(LOWER(sbCustState)) AS a8,
  ANY_VALUE(LOWER(sbCustState)) AS a9
FROM main.sbCustomer
GROUP BY
  1
ORDER BY
  1
