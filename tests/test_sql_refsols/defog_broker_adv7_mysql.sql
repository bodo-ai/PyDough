WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustjoindate AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATETIME)), 2, '0')
    ) AS month,
    COUNT(*) AS n_rows
  FROM main.sbCustomer
  WHERE
    sbcustjoindate < STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    )
    AND sbcustjoindate >= STR_TO_DATE(
      CONCAT(
        YEAR(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '6' MONTH)),
        ' ',
        MONTH(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '6' MONTH)),
        ' 1'
      ),
      '%Y %c %e'
    )
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbCustomer.sbcustjoindate AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(sbCustomer.sbcustjoindate AS DATETIME)), 2, '0')
    ) AS month,
    AVG(sbTransaction.sbtxamount) AS avg_sbTxAmount
  FROM main.sbCustomer AS sbCustomer
  JOIN main.sbTransaction AS sbTransaction
    ON EXTRACT(MONTH FROM CAST(sbCustomer.sbcustjoindate AS DATETIME)) = EXTRACT(MONTH FROM CAST(sbTransaction.sbtxdatetime AS DATETIME))
    AND EXTRACT(YEAR FROM CAST(sbCustomer.sbcustjoindate AS DATETIME)) = EXTRACT(YEAR FROM CAST(sbTransaction.sbtxdatetime AS DATETIME))
    AND sbCustomer.sbcustid = sbTransaction.sbtxcustid
  WHERE
    sbCustomer.sbcustjoindate < STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    )
    AND sbCustomer.sbcustjoindate >= STR_TO_DATE(
      CONCAT(
        YEAR(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '6' MONTH)),
        ' ',
        MONTH(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '6' MONTH)),
        ' 1'
      ),
      '%Y %c %e'
    )
  GROUP BY
    1
)
SELECT
  _s2.month,
  _s2.n_rows AS customer_signups,
  _s3.avg_sbTxAmount AS avg_tx_amount
FROM _s2 AS _s2
JOIN _s3 AS _s3
  ON _s2.month = _s3.month
