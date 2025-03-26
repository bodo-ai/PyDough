WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustjoindate AS join_date
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    MIN(sbtransaction.sbtxdatetime) AS agg_0,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  GROUP BY
    sbtransaction.sbtxcustid
)
SELECT
  _table_alias_0._id AS cust_id,
  CAST((
    (
      (
        CAST((
          JULIANDAY(DATE(_table_alias_1.agg_0, 'start of day')) - JULIANDAY(DATE(_table_alias_0.join_date, 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', _table_alias_1.agg_0) AS INTEGER) - CAST(STRFTIME('%H', _table_alias_0.join_date) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', _table_alias_1.agg_0) AS INTEGER) - CAST(STRFTIME('%M', _table_alias_0.join_date) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', _table_alias_1.agg_0) AS INTEGER) - CAST(STRFTIME('%S', _table_alias_0.join_date) AS INTEGER)
  ) AS REAL) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0._id = _table_alias_1.customer_id
