WITH _t AS (
  SELECT
    accounts.a_balance,
    accounts.a_type,
    customers.c_fname,
    customers.c_lname,
    ROW_NUMBER() OVER (PARTITION BY SUBSTRING(accounts.a_type, -1) || SUBSTRING(accounts.a_type, 1, LENGTH(accounts.a_type) - 1) ORDER BY SQRT(accounts.a_balance) DESC) AS _w
  FROM crbnk.accounts AS accounts
  JOIN crbnk.customers AS customers
    ON accounts.a_custkey = (
      42 - customers.c_key
    )
)
SELECT
  SUBSTRING(a_type, -1) || SUBSTRING(a_type, 1, LENGTH(a_type) - 1) AS account_type,
  SQRT(a_balance) AS balance,
  CONCAT_WS(' ', LOWER(c_fname), LOWER(c_lname)) AS name
FROM _t
WHERE
  _w = 1
