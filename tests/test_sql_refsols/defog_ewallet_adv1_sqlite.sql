WITH _t0 AS (
  SELECT
    COUNT(DISTINCT coupon_id) AS ndistinct_coupon_id,
    COUNT(DISTINCT txid) AS ndistinct_txid,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    status = 'success'
  GROUP BY
    receiver_id
)
SELECT
  merchants.name,
  CAST((
    _t0.ndistinct_coupon_id * 1.0
  ) AS REAL) / _t0.ndistinct_txid AS CPUR
FROM main.merchants AS merchants
JOIN _t0 AS _t0
  ON _t0.receiver_id = merchants.mid
