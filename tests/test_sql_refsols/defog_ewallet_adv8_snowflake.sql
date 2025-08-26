WITH _s1 AS (
  SELECT
    SUM(amount) AS sum_amount,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    receiver_type = 1 AND status = 'success'
  GROUP BY
    2
)
SELECT
  merchants.mid AS merchants_id,
  merchants.name AS merchants_name,
  merchants.category,
  COALESCE(_s1.sum_amount, 0) AS total_revenue,
  ROW_NUMBER() OVER (ORDER BY COALESCE(_s1.sum_amount, 0) DESC) AS mrr
FROM main.merchants AS merchants
JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
