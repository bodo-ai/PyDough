WITH _t AS (
  SELECT
    accounts.a_custkey,
    transactions.t_amount,
    ROW_NUMBER() OVER (PARTITION BY transactions.t_sourceaccount ORDER BY transactions.t_ts) AS _w
  FROM crbnk.accounts AS accounts
  JOIN crbnk.transactions AS transactions
    ON accounts.a_key = transactions.t_sourceaccount
  JOIN crbnk.accounts AS accounts_2
    ON accounts_2.a_key = transactions.t_destaccount
  JOIN crbnk.branches AS branches
    ON SUBSTRING(
      branches.b_addr,
      CASE
        WHEN (
          LENGTH(branches.b_addr) + -4
        ) < 1
        THEN 1
        ELSE (
          LENGTH(branches.b_addr) + -4
        )
      END
    ) = '94105'
    AND accounts_2.a_branchkey = branches.b_key
), _s7 AS (
  SELECT
    a_custkey,
    SUM(t_amount) AS sum_t_amount
  FROM _t
  WHERE
    _w = 1
  GROUP BY
    1
)
SELECT
  customers.c_key AS key,
  CONCAT_WS(' ', customers.c_fname, customers.c_lname) AS name,
  COALESCE(_s7.sum_t_amount, 0) AS first_sends
FROM crbnk.customers AS customers
JOIN _s7 AS _s7
  ON _s7.a_custkey = customers.c_key
ORDER BY
  3 DESC,
  1
LIMIT 3
