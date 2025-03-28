SELECT
  _id,
  name,
  num_tx
FROM (
  SELECT
    COALESCE(agg_0, 0) AS num_tx,
    _id,
    name
  FROM (
    SELECT
      _id,
      agg_0,
      name
    FROM (
      SELECT
        sbCustId AS _id,
        sbCustName AS name
      FROM main.sbCustomer
    )
    LEFT JOIN (
      SELECT
        COUNT() AS agg_0,
        customer_id
      FROM (
        SELECT
          customer_id
        FROM (
          SELECT
            sbTxCustId AS customer_id,
            sbTxDateTime AS date_time,
            sbTxType AS transaction_type
          FROM main.sbTransaction
        )
        WHERE
          (
            DATE_TRUNC('DAY', CAST(date_time AS TIMESTAMP)) = CAST('2023-04-01' AS DATE)
          )
          AND (
            transaction_type = 'sell'
          )
      )
      GROUP BY
        customer_id
    )
      ON _id = customer_id
  )
)
ORDER BY
  num_tx DESC
LIMIT 1
