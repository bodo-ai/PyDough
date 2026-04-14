SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts AS accounts
JOIN bodo.fsi.protected_customers AS protected_customers
  ON NOT protected_customers.firstname IN ('tzuhpuCF', 'cPBnsOl', 'NVGimP')
  AND accounts.customerid = protected_customers.customerid
  AND protected_customers.state IN ('EdJ6cty', 'raXuWJGK', '4o0uuG1', 'FvlL1x8', 'TY84qyAxy', 'AqjyPuvoU8d', 'q6OaWD9X', 'MZBK0 U3nQzZbb', 'lN1sA AANifXzd', 'JXtZBpRhT', 'YYE75')
WHERE
  YEAR(CAST(PTY_UNPROTECT_DOB(accounts.createddate) AS TIMESTAMP)) <= 2022
  AND accounts.currency IN ('jpb', 'gFr')
