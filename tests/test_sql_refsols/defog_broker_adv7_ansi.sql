WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATETIME))), (
          2 * -1
        ))
      END
    ) AS month,
    COUNT(*) AS n_rows
  FROM main.sbcustomer
  WHERE
    sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND sbcustjoindate >= DATE_TRUNC('MONTH', DATE_SUB(CURRENT_TIMESTAMP(), 6, MONTH))
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(
          CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))),
          (
            2 * -1
          )
        )
      END
    ) AS month,
    AVG(sbtransaction.sbtxamount) AS avg_sbtxamount
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)) = EXTRACT(MONTH FROM CAST(sbtransaction.sbtxdatetime AS DATETIME))
    AND EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)) = EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME))
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbcustomer.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND sbcustomer.sbcustjoindate >= DATE_TRUNC('MONTH', DATE_SUB(CURRENT_TIMESTAMP(), 6, MONTH))
  GROUP BY
    1
)
SELECT
  _s2.month,
  _s2.n_rows AS customer_signups,
  _s3.avg_sbtxamount AS avg_tx_amount
FROM _s2 AS _s2
JOIN _s3 AS _s3
  ON _s2.month = _s3.month
