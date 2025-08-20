WITH _t0 AS (
  SELECT
    merchant_id,
    start_date
  FROM main.coupons
), _s1 AS (
  SELECT
    MIN(start_date) AS min_start_date,
    merchant_id
  FROM _t0
  GROUP BY
    2
), _s3 AS (
  SELECT
    MAX(cid) AS max_cid,
    merchant_id,
    start_date
  FROM main.coupons
  GROUP BY
    2,
    3
)
SELECT
  merchants.mid AS merchants_id,
  merchants.created_at AS merchant_registration_date,
  _s1.min_start_date AS earliest_coupon_start_date,
  _s3.max_cid AS earliest_coupon_id
FROM main.merchants AS merchants
LEFT JOIN _s1 AS _s1
  ON _s1.merchant_id = merchants.mid
LEFT JOIN _s3 AS _s3
  ON _s1.min_start_date = _s3.start_date AND _s3.merchant_id = merchants.mid
JOIN _t0 AS _s5
  ON _s5.merchant_id = merchants.mid
  AND _s5.start_date <= DATETIME(merchants.created_at, '1 year')
