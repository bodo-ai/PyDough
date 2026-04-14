SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts AS accounts
JOIN bodo.fsi.protected_customers AS protected_customers
  ON NOT PTY_UNPROTECT(protected_customers.firstname, 'deName') IN ('Jennifer', 'Julio', 'Johnson', 'Jameson', 'Michael', 'Robert')
  AND PTY_UNPROTECT(protected_customers.state, 'deAddress') IN ('Georgia', 'Alabama', 'Mississippi', 'Arkansas', 'Louisiana', 'Florida', 'South Carolina', 'North Carolina', 'Texas', 'Tennessee', 'Missouri')
  AND accounts.customerid = protected_customers.customerid
WHERE
  PTY_UNPROTECT_ACCOUNT(accounts.currency) IN ('USD', 'GPB', 'EUR', 'JPY', 'AUD')
  AND YEAR(CAST(PTY_UNPROTECT_DOB(accounts.createddate) AS TIMESTAMP)) <= 2022
