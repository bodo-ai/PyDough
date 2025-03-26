WITH _table_alias_2 AS (
  SELECT
    COUNT() AS agg_1,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(
          CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))),
          (
            2 * -1
          )
        )
      END
    ) AS month
  FROM main.sbcustomer AS sbcustomer
  WHERE
    sbcustomer.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND sbcustomer.sbcustjoindate >= DATE_TRUNC('MONTH', DATE_ADD(CURRENT_TIMESTAMP(), -6, 'MONTH'))
  GROUP BY
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(
          CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))),
          (
            2 * -1
          )
        )
      END
    )
), _table_alias_0 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))), -2)
      END
    ) AS month,
    EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)) AS join_month,
    EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)) AS join_year,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  WHERE
    sbcustomer.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND sbcustomer.sbcustjoindate >= DATE_TRUNC('MONTH', DATE_ADD(CURRENT_TIMESTAMP(), -6, 'MONTH'))
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
    AND _table_alias_0.join_month = EXTRACT(MONTH FROM CAST(_table_alias_1.date_time AS DATETIME))
    AND _table_alias_0.join_year = EXTRACT(YEAR FROM CAST(_table_alias_1.date_time AS DATETIME))
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
