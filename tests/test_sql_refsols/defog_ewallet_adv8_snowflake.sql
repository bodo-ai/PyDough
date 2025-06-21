WITH _S1 AS (
  SELECT
    SUM(amount) AS AGG_0,
    receiver_id AS RECEIVER_ID
  FROM MAIN.WALLET_TRANSACTIONS_DAILY
  WHERE
    receiver_type = 1 AND status = 'success'
  GROUP BY
    receiver_id
)
SELECT
  MERCHANTS.mid AS merchants_id,
  MERCHANTS.name AS merchants_name,
  MERCHANTS.category,
  COALESCE(_S1.AGG_0, 0) AS total_revenue,
  ROW_NUMBER() OVER (ORDER BY COALESCE(_S1.AGG_0, 0) DESC) AS mrr
FROM MAIN.MERCHANTS AS MERCHANTS
JOIN _S1 AS _S1
  ON MERCHANTS.mid = _S1.RECEIVER_ID
