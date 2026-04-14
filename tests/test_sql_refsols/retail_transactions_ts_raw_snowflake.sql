SELECT
  COUNT_IF(
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 1
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 7
  ) AS n1,
  COUNT_IF(
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 2
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 7
  ) AS n2,
  COUNT_IF(
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 1
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 8
  ) AS n3,
  COUNT_IF(
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 2
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 8
  ) AS n4,
  COUNT_IF(
    (
      DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) < 4
      OR MINUTE(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = SECOND(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP))
    )
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) < 3
  ) AS n5
FROM bodo.retail.transactions
WHERE
  (
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) < 4
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) < 3
  )
  OR (
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 1
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 7
  )
  OR (
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 1
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 8
  )
  OR (
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 2
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 7
  )
  OR (
    DAY(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 2
    AND HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 8
  )
  OR (
    HOUR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) < 3
    AND MINUTE(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = SECOND(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP))
  )
