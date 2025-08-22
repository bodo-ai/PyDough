WITH _t AS (
  SELECT
    a_balance,
    a_custkey,
    a_type,
    ROW_NUMBER() OVER (PARTITION BY a_type ORDER BY a_balance DESC) AS _w
  FROM crbnk.accounts
)
SELECT
  _t.a_type AS account_type,
  _t.a_balance AS balance,
  CONCAT_WS(' ', customers.c_fname, customers.c_lname) AS name
FROM _t AS _t
JOIN crbnk.customers AS customers
  ON _t.a_custkey = customers.c_key
WHERE
  _t._w = 1
