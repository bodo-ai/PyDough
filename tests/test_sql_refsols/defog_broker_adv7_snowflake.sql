WITH _S2 AS (
  SELECT
    COUNT(*) AS AGG_1,
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, sbcustjoindate),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, sbcustjoindate)) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', DATE_PART(MONTH, sbcustjoindate)), (
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
      DATE_PART(YEAR, sbcustjoindate),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, sbcustjoindate)) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', DATE_PART(MONTH, sbcustjoindate)), (
          2 * -1
        ))
      END
    )
), _S3 AS (
  SELECT
    AVG(SBTRANSACTION.sbtxamount) AS AGG_0,
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, SBCUSTOMER.sbcustjoindate),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, SBCUSTOMER.sbcustjoindate)) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, SBCUSTOMER.sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', DATE_PART(MONTH, SBCUSTOMER.sbcustjoindate)), (
          2 * -1
        ))
      END
    ) AS MONTH
  FROM MAIN.SBCUSTOMER AS SBCUSTOMER
  JOIN MAIN.SBTRANSACTION AS SBTRANSACTION
    ON DATE_PART(MONTH, SBCUSTOMER.sbcustjoindate) = DATE_PART(MONTH, SBTRANSACTION.sbtxdatetime)
    AND DATE_PART(YEAR, SBCUSTOMER.sbcustjoindate) = DATE_PART(YEAR, SBTRANSACTION.sbtxdatetime)
    AND SBCUSTOMER.sbcustid = SBTRANSACTION.sbtxcustid
  WHERE
    SBCUSTOMER.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND SBCUSTOMER.sbcustjoindate >= DATE_TRUNC('MONTH', DATEADD(MONTH, -6, CURRENT_TIMESTAMP()))
  GROUP BY
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, SBCUSTOMER.sbcustjoindate),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, SBCUSTOMER.sbcustjoindate)) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, SBCUSTOMER.sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', DATE_PART(MONTH, SBCUSTOMER.sbcustjoindate)), (
          2 * -1
        ))
      END
    )
)
SELECT
  _S2.MONTH AS month,
  _S2.AGG_1 AS customer_signups,
  _S3.AGG_0 AS avg_tx_amount
FROM _S2 AS _S2
LEFT JOIN _S3 AS _S3
  ON _S2.MONTH = _S3.MONTH
