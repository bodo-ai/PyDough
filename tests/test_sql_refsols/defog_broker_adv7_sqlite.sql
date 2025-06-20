WITH _s5 AS (
  SELECT
    COUNT(*) AS agg_1,
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
), _s6 AS (
  SELECT
    AVG(_s2.sbtxamount) AS agg_0,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', _s1.sbcustjoindate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', _s1.sbcustjoindate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', _s1.sbcustjoindate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', _s1.sbcustjoindate) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbcustomer AS _s1
  JOIN main.sbtransaction AS _s2
    ON CAST(STRFTIME('%Y', _s1.sbcustjoindate) AS INTEGER) = CAST(STRFTIME('%Y', _s2.sbtxdatetime) AS INTEGER)
    AND CAST(STRFTIME('%m', _s1.sbcustjoindate) AS INTEGER) = CAST(STRFTIME('%m', _s2.sbtxdatetime) AS INTEGER)
    AND _s1.sbcustid = _s2.sbtxcustid
  WHERE
    _s1.sbcustjoindate < DATE('now', 'start of month')
    AND _s1.sbcustjoindate >= DATE(DATETIME('now', '-6 month'), 'start of month')
  GROUP BY
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', _s1.sbcustjoindate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', _s1.sbcustjoindate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', _s1.sbcustjoindate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', _s1.sbcustjoindate) AS INTEGER), (
          2 * -1
        ))
      END
    )
)
SELECT
  _s5.month,
  _s5.agg_1 AS customer_signups,
  _s6.agg_0 AS avg_tx_amount
FROM _s5 AS _s5
LEFT JOIN _s6 AS _s6
  ON _s5.month = _s6.month
