SELECT
  _id,
  name,
  num_tx
FROM (
  SELECT
    _id,
    name,
    num_tx,
    ordering_1
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS num_tx,
      COALESCE(agg_0, 0) AS ordering_1,
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
              DATE(date_time, 'start of day') = '2023-04-01'
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
    ordering_1 DESC
  LIMIT 1
)
ORDER BY
  ordering_1 DESC
