SELECT
  COUNT() AS n_customers
FROM (
  SELECT
    (NULL)
  FROM (
    SELECT
      _id
    FROM (
      SELECT
        sbCustEmail AS email,
        sbCustId AS _id
      FROM main.sbCustomer
    )
    WHERE
      email LIKE '%.com'
  )
  SEMI JOIN (
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
      )
      LEFT JOIN (
        SELECT
          sbTickerId AS _id,
          sbTickerSymbol AS symbol
        FROM main.sbTicker
      )
        ON ticker_id = _id
    )
    WHERE
      symbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
  )
    ON _id = customer_id
)
