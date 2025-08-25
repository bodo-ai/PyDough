WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustjoindate AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATETIME)), 2, '0')
    ) AS month,
    COUNT(*) AS n_rows
  FROM main.sbcustomer
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
    1
), _s3 AS (
  SELECT
    AVG(sbtransaction.sbtxamount) AS avg_sbtxamount,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)), 2, '0')
    ) AS month
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)) = EXTRACT(MONTH FROM CAST(sbtransaction.sbtxdatetime AS DATETIME))
    AND EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)) = EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME))
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbcustomer.sbcustjoindate < STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    )
    AND sbcustomer.sbcustjoindate >= STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-6' MONTH)),
        ' ',
        MONTH(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-6' MONTH)),
        ' 1'
      ),
      '%Y %c %e'
    )
  GROUP BY
    2
)
SELECT
  _s2.month,
  _s2.n_rows AS customer_signups,
  _s3.avg_sbtxamount AS avg_tx_amount
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.month = _s3.month
