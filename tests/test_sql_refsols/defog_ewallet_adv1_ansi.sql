WITH _s1 AS (
  SELECT
    COUNT(DISTINCT coupon_id) AS ndistinct_coupon_id,
    COUNT(DISTINCT txid) AS ndistinct_txid,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    status = 'success'
  GROUP BY
    3
)
SELECT
  merchants.name,
  (
    _s1.ndistinct_coupon_id * 1.0
  ) / _s1.ndistinct_txid AS CPUR
FROM main.merchants AS merchants
JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
