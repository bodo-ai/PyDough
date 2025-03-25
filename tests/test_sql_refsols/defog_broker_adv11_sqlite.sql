SELECT
  COUNT() AS n_customers
FROM (
  SELECT
    _id
  FROM (
    SELECT
      _id
    FROM (
      SELECT
        sbCustEmail AS email,
        sbCustId AS _id
      FROM main.sbCustomer
    ) AS _t1
    WHERE
      email LIKE '%.com'
  ) AS _table_alias_2
  WHERE
    EXISTS(
      SELECT
        1
      FROM (
        SELECT
          customer_id
        FROM (
          SELECT
            customer_id,
            symbol
          FROM (
            SELECT
              sbTxCustId AS customer_id,
              sbTxTickerId AS ticker_id
            FROM main.sbTransaction
          ) AS _table_alias_0
          LEFT JOIN (
            SELECT
              sbTickerId AS _id,
              sbTickerSymbol AS symbol
            FROM main.sbTicker
          ) AS _table_alias_1
            ON ticker_id = _id
        ) AS _t2
        WHERE
          symbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
      ) AS _table_alias_3
      WHERE
        _id = customer_id
    )
) AS _t0
