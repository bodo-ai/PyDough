SELECT
  _id AS cust_id,
  CAST((
    (
      (
        CAST((JULIANDAY(DATE(agg_0, 'start of day')) - JULIANDAY(DATE(join_date, 'start of day'))) AS INTEGER) * 24 + CAST(STRFTIME('%H', agg_0) AS INTEGER) - CAST(STRFTIME('%H', join_date) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', agg_0) AS INTEGER) - CAST(STRFTIME('%M', join_date) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', agg_0) AS INTEGER) - CAST(STRFTIME('%S', join_date) AS INTEGER)
  ) AS REAL) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM (
  SELECT
    _id,
    agg_0,
    join_date
  FROM (
    SELECT
      sbCustId AS _id,
      sbCustJoinDate AS join_date
    FROM main.sbCustomer
  )
  INNER JOIN (
    SELECT
      MIN(date_time) AS agg_0,
      customer_id
    FROM (
      SELECT
        sbTxCustId AS customer_id,
        sbTxDateTime AS date_time
      FROM main.sbTransaction
    )
    GROUP BY
      customer_id
  )
    ON _id = customer_id
)
