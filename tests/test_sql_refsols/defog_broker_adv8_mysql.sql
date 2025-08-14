WITH _u_0 AS (
  SELECT
    sbcustid AS _u_1
  FROM main.sbCustomer
  WHERE
    LOWER(sbcustcountry) = 'usa'
  GROUP BY
    sbcustid
)
SELECT
  CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE NULL END AS n_transactions,
  COALESCE(SUM(sbTransaction.sbtxamount), 0) AS total_amount
FROM main.sbTransaction AS sbTransaction
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbTransaction.sbtxcustid
WHERE
  NOT _u_0._u_1 IS NULL
  AND sbTransaction.sbtxdatetime < DATE(
    DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    )
  )
  AND sbTransaction.sbtxdatetime >= DATE_ADD(
    DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    ),
    INTERVAL '-1' WEEK
  )
