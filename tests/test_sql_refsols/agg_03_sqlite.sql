WITH _t AS (
  SELECT
    a_balance,
    a_type,
    ROW_NUMBER() OVER (PARTITION BY a_type ORDER BY a_balance DESC) AS _w
  FROM crbnk.accounts
)
SELECT
  a_type AS account_type,
  a_balance AS balance
FROM _t
WHERE
  _w = 1
