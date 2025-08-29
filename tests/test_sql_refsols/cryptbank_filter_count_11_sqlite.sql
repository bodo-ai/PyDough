SELECT
  COUNT(*) AS n
FROM crbnk.transactions AS transactions
JOIN crbnk.accounts AS accounts
  ON accounts.a_key = transactions.t_sourceaccount
JOIN crbnk.customers AS customers
  ON accounts.a_custkey = customers.c_key AND customers.c_fname = 'alice'
