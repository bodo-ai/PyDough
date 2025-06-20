WITH _s5 AS (
  SELECT
    COUNT(*) AS agg_1,
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
), _s6 AS (
  SELECT
    AVG(_s2.sbtxamount) AS agg_0,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM _s1.sbcustjoindate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM _s1.sbcustjoindate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM _s1.sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM _s1.sbcustjoindate)), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbcustomer AS _s1
  JOIN main.sbtransaction AS _s2
    ON EXTRACT(MONTH FROM _s1.sbcustjoindate) = EXTRACT(MONTH FROM _s2.sbtxdatetime)
    AND EXTRACT(YEAR FROM _s1.sbcustjoindate) = EXTRACT(YEAR FROM _s2.sbtxdatetime)
    AND _s1.sbcustid = _s2.sbtxcustid
  WHERE
    _s1.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND _s1.sbcustjoindate >= DATE_TRUNC('MONTH', DATE_ADD(CURRENT_TIMESTAMP(), -6, 'MONTH'))
  GROUP BY
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM _s1.sbcustjoindate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM _s1.sbcustjoindate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM _s1.sbcustjoindate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM _s1.sbcustjoindate)), (
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
