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
      ) AS _table_alias_0
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
          ) AS _t4
          WHERE
            (
              DATE_TRUNC('DAY', CAST(date_time AS TIMESTAMP)) = CAST('2023-04-01' AS DATE)
            )
            AND (
              transaction_type = 'sell'
            )
        ) AS _t3
        GROUP BY
          customer_id
      ) AS _table_alias_1
        ON _id = customer_id
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC
  LIMIT 1
) AS _t0
ORDER BY
  ordering_1 DESC
