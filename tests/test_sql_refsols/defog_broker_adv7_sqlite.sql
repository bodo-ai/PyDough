WITH _table_alias_2 AS (
  SELECT
    COUNT() AS agg_1,
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
), _table_alias_0 AS (
  SELECT
    CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER) AS join_year,
    CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER) AS join_month,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER), -2)
      END
    ) AS month,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  WHERE
    sbcustomer.sbcustjoindate < DATE('now', 'start of month')
    AND sbcustomer.sbcustjoindate >= DATE(DATETIME('now', '-6 month'), 'start of month')
), _table_alias_1 AS (
  SELECT
    sbtransaction.sbtxamount AS amount,
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxdatetime AS date_time
  FROM main.sbtransaction AS sbtransaction
), _table_alias_3 AS (
  SELECT
    AVG(_table_alias_1.amount) AS agg_0,
    _table_alias_0.month AS month
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0._id = _table_alias_1.customer_id
    AND _table_alias_0.join_month = CAST(STRFTIME('%m', _table_alias_1.date_time) AS INTEGER)
    AND _table_alias_0.join_year = CAST(STRFTIME('%Y', _table_alias_1.date_time) AS INTEGER)
  GROUP BY
    _table_alias_0.month
)
SELECT
  _table_alias_2.month AS month,
  COALESCE(_table_alias_2.agg_1, 0) AS customer_signups,
  _table_alias_3.agg_0 AS avg_tx_amount
FROM _table_alias_2 AS _table_alias_2
LEFT JOIN _table_alias_3 AS _table_alias_3
  ON _table_alias_2.month = _table_alias_3.month
