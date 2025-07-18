WITH _s2 AS (
  SELECT
    COUNT(*) AS n_rows,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbcustjoindate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbcustjoindate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbcustjoindate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbcustjoindate) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbcustomer
  WHERE
    sbcustjoindate < DATE('now', 'start of month')
    AND sbcustjoindate >= DATE(DATETIME('now', '-6 month'), 'start of month')
  GROUP BY
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbcustjoindate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbcustjoindate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbcustjoindate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbcustjoindate) AS INTEGER), (
          2 * -1
        ))
      END
    )
), _s3 AS (
  SELECT
    AVG(sbtransaction.sbtxamount) AS avg_sbtxamount,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER) = CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER)
    AND CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER) = CAST(STRFTIME('%m', sbtransaction.sbtxdatetime) AS INTEGER)
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbcustomer.sbcustjoindate < DATE('now', 'start of month')
    AND sbcustomer.sbcustjoindate >= DATE(DATETIME('now', '-6 month'), 'start of month')
  GROUP BY
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER), (
          2 * -1
        ))
      END
    )
)
SELECT
  _s2.month,
  _s2.n_rows AS customer_signups,
  _s3.avg_sbtxamount AS avg_tx_amount
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.month = _s3.month
