WITH _t0 AS (
  SELECT
    COUNT(DISTINCT coupon_id) AS agg_0,
    COUNT(DISTINCT txid) AS agg_1,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    status = 'success'
  GROUP BY
    receiver_id
)
SELECT
  _s0.name,
  (
    _t0.agg_0 * 1.0
  ) / _t0.agg_1 AS CPUR
FROM main.merchants AS _s0
JOIN _t0 AS _t0
  ON _s0.mid = _t0.receiver_id
