WITH _t2_2 AS (
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
), _t3_2 AS (
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
    ON EXTRACT(MONTH FROM sbcustomer.sbcustjoindate) = EXTRACT(MONTH FROM sbtransaction.sbtxdatetime)
    AND EXTRACT(YEAR FROM sbcustomer.sbcustjoindate) = EXTRACT(YEAR FROM sbtransaction.sbtxdatetime)
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbcustomer.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
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
  _t2.month AS month,
  COALESCE(_t2.agg_1, 0) AS customer_signups,
  _t3.agg_0 AS avg_tx_amount
FROM _t2_2 AS _t2
LEFT JOIN _t3_2 AS _t3
  ON _t2.month = _t3.month
