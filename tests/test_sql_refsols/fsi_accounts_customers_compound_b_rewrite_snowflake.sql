SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts AS accounts
JOIN bodo.fsi.protected_customers AS protected_customers
  ON NOT protected_customers.firstname IN ('Jennifer', 'Julio', 'Johnson', 'Jameson', 'Michael', 'Robert')
  AND accounts.customerid = protected_customers.customerid
  AND protected_customers.state IN ('Georgia', 'Alabama', 'Mississippi', 'Arkansas', 'Louisiana', 'Florida', 'South Carolina', 'North Carolina', 'Texas', 'Tennessee', 'Missouri')
WHERE
  YEAR(CAST(accounts.createddate AS TIMESTAMP)) <= 2022
  AND accounts.currency IN ('USD', 'GPB', 'EUR', 'JPY', 'AUD')
