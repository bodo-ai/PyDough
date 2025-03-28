WITH _table_alias_2 AS (
  SELECT
    COUNT() AS agg_1,
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
), _table_alias_3 AS (
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
  _table_alias_2.month AS month,
  COALESCE(_table_alias_2.agg_1, 0) AS customer_signups,
  _table_alias_3.agg_0 AS avg_tx_amount
FROM _table_alias_2 AS _table_alias_2
LEFT JOIN _table_alias_3 AS _table_alias_3
  ON _table_alias_2.month = _table_alias_3.month
