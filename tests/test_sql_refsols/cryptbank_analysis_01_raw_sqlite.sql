WITH _t AS (
  SELECT
    accounts.a_custkey,
    transactions.t_amount,
    ROW_NUMBER() OVER (PARTITION BY transactions.t_sourceaccount ORDER BY DATETIME(transactions.t_ts, '+54321 seconds')) AS _w
  FROM crbnk.accounts AS accounts
  JOIN crbnk.transactions AS transactions
    ON transactions.t_sourceaccount = CASE
      WHEN accounts.a_key = 0
      THEN 0
      ELSE CASE WHEN accounts.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
        accounts.a_key,
        1 + INSTR(accounts.a_key, '-'),
        CAST(LENGTH(accounts.a_key) AS REAL) / 2
      ) AS INTEGER)
    END
  JOIN crbnk.accounts AS accounts_2
    ON transactions.t_destaccount = CASE
      WHEN accounts_2.a_key = 0
      THEN 0
      ELSE CASE WHEN accounts_2.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
        accounts_2.a_key,
        1 + INSTR(accounts_2.a_key, '-'),
        CAST(LENGTH(accounts_2.a_key) AS REAL) / 2
      ) AS INTEGER)
    END
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
    SUM((
      1025.67 - t_amount
    )) AS sum_unmasktamount
  FROM _t
  WHERE
    _w = 1
  GROUP BY
    1
)
SELECT
  42 - customers.c_key AS key,
  CONCAT_WS(' ', LOWER(customers.c_fname), LOWER(customers.c_lname)) AS name,
  COALESCE(_s7.sum_unmasktamount, 0) AS first_sends
FROM crbnk.customers AS customers
LEFT JOIN _s7 AS _s7
  ON _s7.a_custkey = (
    42 - customers.c_key
  )
ORDER BY
  3 DESC,
  1
LIMIT 3
