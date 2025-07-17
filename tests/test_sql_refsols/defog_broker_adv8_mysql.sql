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
  AND sbTransaction.sbtxdatetime < STR_TO_DATE(
    CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', WEEK(CURRENT_TIMESTAMP(), 1), ' 1'),
    '%Y %u %w'
  )
  AND sbTransaction.sbtxdatetime >= DATE_ADD(
    STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', WEEK(CURRENT_TIMESTAMP(), 1), ' 1'),
      '%Y %u %w'
    ),
    INTERVAL '-1' WEEK
  )
