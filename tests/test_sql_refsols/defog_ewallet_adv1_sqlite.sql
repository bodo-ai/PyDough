WITH _s1 AS (
  SELECT
    CAST((
      COUNT(DISTINCT coupon_id) * 1.0
    ) AS REAL) / COUNT(DISTINCT txid) AS cpur,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    status = 'success'
  GROUP BY
    receiver_id
)
SELECT
  merchants.name,
  _s1.cpur AS CPUR
FROM main.merchants AS merchants
JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
