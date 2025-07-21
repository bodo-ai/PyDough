WITH _s2 AS (
  SELECT
    COUNT(*) AS n_rows,
    CONCAT_WS('-', YEAR(sbcustjoindate), LPAD(MONTH(sbcustjoindate), 2, '0')) AS month
  FROM main.sbCustomer
  WHERE
    sbcustjoindate < STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    )
    AND sbcustjoindate >= STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-6' MONTH)),
        ' ',
        MONTH(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-6' MONTH)),
        ' 1'
      ),
      '%Y %c %e'
    )
  GROUP BY
    CONCAT_WS('-', YEAR(sbcustjoindate), LPAD(MONTH(sbcustjoindate), 2, '0'))
), _s3 AS (
  SELECT
    AVG(sbTransaction.sbtxamount) AS avg_sbTxAmount,
    CONCAT_WS('-', YEAR(sbCustomer.sbcustjoindate), LPAD(MONTH(sbCustomer.sbcustjoindate), 2, '0')) AS month
  FROM main.sbCustomer AS sbCustomer
  JOIN main.sbTransaction AS sbTransaction
    ON MONTH(sbCustomer.sbcustjoindate) = MONTH(sbTransaction.sbtxdatetime)
    AND YEAR(sbCustomer.sbcustjoindate) = YEAR(sbTransaction.sbtxdatetime)
    AND sbCustomer.sbcustid = sbTransaction.sbtxcustid
  WHERE
    sbCustomer.sbcustjoindate < STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    )
    AND sbCustomer.sbcustjoindate >= STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-6' MONTH)),
        ' ',
        MONTH(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-6' MONTH)),
        ' 1'
      ),
      '%Y %c %e'
    )
  GROUP BY
    CONCAT_WS('-', YEAR(sbCustomer.sbcustjoindate), LPAD(MONTH(sbCustomer.sbcustjoindate), 2, '0'))
)
SELECT
  _s2.month,
  _s2.n_rows AS customer_signups,
  _s3.avg_sbTxAmount AS avg_tx_amount
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.month = _s3.month
