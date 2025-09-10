WITH _s3 AS (
  SELECT
    t_sourceaccount,
    COUNT(*) AS n_rows
  FROM crbnk.transactions
  WHERE
    t_amount > 9000.0
  GROUP BY
    1
)
SELECT
  accounts.a_key AS key,
  CONCAT_WS(' ', customers.c_fname, customers.c_lname) AS cust_name,
  _s3.n_rows AS n_trans
FROM crbnk.accounts AS accounts
JOIN crbnk.customers AS customers
  ON CAST(STRFTIME('%Y', customers.c_birthday) AS INTEGER) <= 1985
  AND CAST(STRFTIME('%Y', customers.c_birthday) AS INTEGER) >= 1980
  AND accounts.a_custkey = customers.c_key
JOIN _s3 AS _s3
  ON _s3.t_sourceaccount = accounts.a_key
ORDER BY
  1
