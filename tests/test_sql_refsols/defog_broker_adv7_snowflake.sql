WITH _S2 AS (
  SELECT
    COUNT(*) AS N_ROWS,
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, CAST(sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, CAST(sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, CAST(sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(CONCAT('00', DATE_PART(MONTH, CAST(sbcustjoindate AS DATETIME))), (
          2 * -1
        ))
      END
    ) AS MONTH
  FROM MAIN.SBCUSTOMER
  WHERE
    sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND sbcustjoindate >= DATE_TRUNC('MONTH', DATEADD(MONTH, -6, CURRENT_TIMESTAMP()))
  GROUP BY
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, CAST(sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, CAST(sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, CAST(sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(CONCAT('00', DATE_PART(MONTH, CAST(sbcustjoindate AS DATETIME))), (
          2 * -1
        ))
      END
    )
), _S3 AS (
  SELECT
    AVG(SBTRANSACTION.sbtxamount) AS AVG_SBTXAMOUNT,
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(
          CONCAT('00', DATE_PART(MONTH, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME))),
          (
            2 * -1
          )
        )
      END
    ) AS MONTH
  FROM MAIN.SBCUSTOMER AS SBCUSTOMER
  JOIN MAIN.SBTRANSACTION AS SBTRANSACTION
    ON DATE_PART(MONTH, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME)) = DATE_PART(MONTH, CAST(SBTRANSACTION.sbtxdatetime AS DATETIME))
    AND DATE_PART(YEAR, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME)) = DATE_PART(YEAR, CAST(SBTRANSACTION.sbtxdatetime AS DATETIME))
    AND SBCUSTOMER.sbcustid = SBTRANSACTION.sbtxcustid
  WHERE
    SBCUSTOMER.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND SBCUSTOMER.sbcustjoindate >= DATE_TRUNC('MONTH', DATEADD(MONTH, -6, CURRENT_TIMESTAMP()))
  GROUP BY
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(
          CONCAT('00', DATE_PART(MONTH, CAST(SBCUSTOMER.sbcustjoindate AS DATETIME))),
          (
            2 * -1
          )
        )
      END
    )
)
SELECT
  _S2.MONTH AS month,
  _S2.N_ROWS AS customer_signups,
  _S3.AVG_SBTXAMOUNT AS avg_tx_amount
FROM _S2 AS _S2
LEFT JOIN _S3 AS _S3
  ON _S2.MONTH = _S3.MONTH
