WITH _t3 AS (
  SELECT DISTINCT
    PTY_UNPROTECT(state, 'deAddress') AS unmask_state
  FROM bodo.fsi.protected_customers
), _s0 AS (
  SELECT
    unmask_state
  FROM _t3
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _t1 AS (
  SELECT
    accounts.balance,
    protected_customers.firstname,
    protected_customers.lastname,
    _s0.unmask_state
  FROM _s0 AS _s0
  JOIN bodo.fsi.protected_customers AS protected_customers
    ON _s0.unmask_state = PTY_UNPROTECT(protected_customers.state, 'deAddress')
  JOIN bodo.fsi.accounts AS accounts
    ON accounts.customerid = protected_customers.customerid
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY unmask_state ORDER BY accounts.balance DESC) = 1
)
SELECT
  unmask_state AS state,
  balance,
  PTY_UNPROTECT(firstname, 'deName') AS first_name,
  PTY_UNPROTECT(lastname, 'deName') AS last_name
FROM _t1
ORDER BY
  1 NULLS FIRST
