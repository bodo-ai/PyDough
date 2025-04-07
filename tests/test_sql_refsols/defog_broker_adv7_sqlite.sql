WITH _t2_2 AS (
  SELECT
    COUNT() AS agg_1,
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
), _t3_2 AS (
  SELECT
    AVG(sbtransaction.sbtxamount) AS agg_0,
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
  _t2.month,
  COALESCE(_t2.agg_1, 0) AS customer_signups,
  _t3.agg_0 AS avg_tx_amount
FROM _t2_2 AS _t2
LEFT JOIN _t3_2 AS _t3
  ON _t2.month = _t3.month
