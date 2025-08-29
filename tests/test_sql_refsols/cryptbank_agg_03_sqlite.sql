WITH _t AS (
  SELECT
    accounts.a_balance,
    accounts.a_type,
    customers.c_fname,
    customers.c_lname,
    ROW_NUMBER() OVER (PARTITION BY accounts.a_type ORDER BY accounts.a_balance DESC) AS _w
  FROM crbnk.accounts AS accounts
  JOIN crbnk.customers AS customers
    ON accounts.a_custkey = customers.c_key
)
SELECT
  a_type AS account_type,
  a_balance AS balance,
  CONCAT_WS(' ', c_fname, c_lname) AS name
FROM _t
WHERE
  _w = 1
