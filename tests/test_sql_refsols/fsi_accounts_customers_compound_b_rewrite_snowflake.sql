SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts AS accounts
JOIN bodo.fsi.protected_customers AS protected_customers
  ON NOT protected_customers.firstname IN (PTY_PROTECT('Jennifer', 'deName'), PTY_PROTECT('Julio', 'deName'), PTY_PROTECT('Johnson', 'deName'), PTY_PROTECT('Jameson', 'deName'), PTY_PROTECT('Michael', 'deName'), PTY_PROTECT('Robert', 'deName'))
  AND PTY_UNPROTECT(protected_customers.customerid, 'deAccount') = PTY_UNPROTECT_ACCOUNT(accounts.customerid)
  AND protected_customers.state IN (PTY_PROTECT('Georgia', 'deAddress'), PTY_PROTECT('Alabama', 'deAddress'), PTY_PROTECT('Mississippi', 'deAddress'), PTY_PROTECT('Arkansas', 'deAddress'), PTY_PROTECT('Louisiana', 'deAddress'), PTY_PROTECT('Florida', 'deAddress'), PTY_PROTECT('South Carolina', 'deAddress'), PTY_PROTECT('North Carolina', 'deAddress'), PTY_PROTECT('Texas', 'deAddress'), PTY_PROTECT('Tennessee', 'deAddress'), PTY_PROTECT('Missouri', 'deAddress'))
WHERE
  YEAR(CAST(PTY_UNPROTECT_DOB(accounts.createddate) AS TIMESTAMP)) <= 2022
  AND accounts.currency IN (PTY_PROTECT('USD', 'deAccount'), PTY_PROTECT('GPB', 'deAccount'), PTY_PROTECT('EUR', 'deAccount'), PTY_PROTECT('JPY', 'deAccount'), PTY_PROTECT('AUD', 'deAccount'))
