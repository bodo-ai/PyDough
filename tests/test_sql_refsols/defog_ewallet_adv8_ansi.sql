WITH _s3 AS (
  SELECT
    SUM(amount) AS agg_0,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    receiver_type = 1 AND status = 'success'
  GROUP BY
    receiver_id
)
SELECT
  _s0.mid AS merchants_id,
  _s0.name AS merchants_name,
  _s0.category,
  COALESCE(_s3.agg_0, 0) AS total_revenue,
  ROW_NUMBER() OVER (ORDER BY COALESCE(_s3.agg_0, 0) DESC NULLS FIRST) AS mrr
FROM main.merchants AS _s0
JOIN _s3 AS _s3
  ON _s0.mid = _s3.receiver_id
