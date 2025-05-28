WITH _s2 AS (
  SELECT
    COUNT() AS agg_1,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbcustjoindate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbcustjoindate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbcustjoindate)), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbcustomer
  WHERE
    sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND sbcustjoindate >= DATE_TRUNC('MONTH', DATE_ADD(CURRENT_TIMESTAMP(), -6, 'MONTH'))
  GROUP BY
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbcustjoindate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbcustjoindate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbcustjoindate)), (
          2 * -1
        ))
      END
    )
), _s3 AS (
  SELECT
    AVG(sbtransaction.sbtxamount) AS agg_0,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbcustomer.sbcustjoindate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbcustomer.sbcustjoindate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbcustomer.sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbcustomer.sbcustjoindate)), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    EXTRACT(MONTH FROM sbcustomer.sbcustjoindate) = EXTRACT(MONTH FROM sbtransaction.sbtxdatetime)
    AND EXTRACT(YEAR FROM sbcustomer.sbcustjoindate) = EXTRACT(YEAR FROM sbtransaction.sbtxdatetime)
    AND sbcustomer.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND sbcustomer.sbcustjoindate >= DATE_TRUNC('MONTH', DATE_ADD(CURRENT_TIMESTAMP(), -6, 'MONTH'))
  GROUP BY
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbcustomer.sbcustjoindate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbcustomer.sbcustjoindate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbcustomer.sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbcustomer.sbcustjoindate)), (
          2 * -1
        ))
      END
    )
)
SELECT
  _s2.month,
  _s2.agg_1 AS customer_signups,
  _s3.agg_0 AS avg_tx_amount
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.month = _s3.month
