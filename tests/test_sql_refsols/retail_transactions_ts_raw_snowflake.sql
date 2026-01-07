WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM bodo.retail.transactions
  WHERE
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 1
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 7
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM bodo.retail.transactions
  WHERE
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 2
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 7
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM bodo.retail.transactions
  WHERE
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 1
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 8
), _s5 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM bodo.retail.transactions
  WHERE
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 2
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 8
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM bodo.retail.transactions
  WHERE
    (
      DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) < 4
      OR MINUTE(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = SECOND(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP))
    )
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) < 3
)
SELECT
  _s0.n_rows AS n1,
  _s1.n_rows AS n2,
  _s3.n_rows AS n3,
  _s5.n_rows AS n4,
  _s7.n_rows AS n5
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
CROSS JOIN _s3 AS _s3
CROSS JOIN _s5 AS _s5
CROSS JOIN _s7 AS _s7
