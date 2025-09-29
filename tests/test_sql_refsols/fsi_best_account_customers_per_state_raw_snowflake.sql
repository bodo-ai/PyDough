WITH _s0 AS (
  SELECT DISTINCT
    state
  FROM bodo.fsi.protected_customers
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _t0 AS (
  SELECT
    accounts.balance,
    protected_customers.firstname,
    protected_customers.lastname,
    _s0.state
  FROM _s0 AS _s0
  JOIN bodo.fsi.protected_customers AS protected_customers
    ON _s0.state = protected_customers.state
  JOIN bodo.fsi.accounts AS accounts
    ON accounts.customerid = protected_customers.customerid
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY state ORDER BY accounts.balance DESC) = 1
)
SELECT
  state,
  balance,
  firstname AS first_name,
  lastname AS last_name
FROM _t0
ORDER BY
  1 NULLS FIRST
