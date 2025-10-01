WITH _s1 AS (
  SELECT
    receiver_id,
    COUNT(DISTINCT coupon_id) AS ndistinct_coupon_id,
    COUNT(DISTINCT txid) AS ndistinct_txid
  FROM main.wallet_transactions_daily
  WHERE
    status = 'success'
  GROUP BY
    1
)
SELECT
  merchants.name,
  CAST(_s1.ndistinct_coupon_id AS DOUBLE PRECISION) / _s1.ndistinct_txid AS CPUR
FROM main.merchants AS merchants
JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
