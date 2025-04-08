WITH _t0 AS (
  SELECT
    merchant_id,
    start_date
  FROM main.coupons
), _s1 AS (
  SELECT
    MIN(start_date) AS agg_0,
    merchant_id
  FROM _t0
  GROUP BY
    merchant_id
), _s3 AS (
  SELECT
    MIN(start_date) AS agg_0,
    merchant_id
  FROM _t0
  GROUP BY
    merchant_id
), _s5 AS (
  SELECT
    MAX(cid) AS agg_1,
    merchant_id,
    start_date
  FROM main.coupons
  GROUP BY
    merchant_id,
    start_date
)
SELECT
  merchants.mid AS merchants_id,
  merchants.created_at AS merchant_registration_date,
  _s1.agg_0 AS earliest_coupon_start_date,
  _s5.agg_1 AS earliest_coupon_id
FROM main.merchants AS merchants
LEFT JOIN _s1 AS _s1
  ON _s1.merchant_id = merchants.mid
LEFT JOIN main.merchants AS merchants_2
  ON merchants.mid = merchants_2.mid
LEFT JOIN _s3 AS _s3
  ON _s3.merchant_id = merchants_2.mid
JOIN _s5 AS _s5
  ON _s3.agg_0 = _s5.start_date AND _s5.merchant_id = merchants_2.mid
JOIN _t0 AS _s9
  ON _s9.merchant_id = merchants.mid
  AND _s9.start_date <= DATETIME(merchants.created_at, '1 year')
