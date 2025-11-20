WITH _t0 AS (
  SELECT
    merchant_id,
    start_date
  FROM main.coupons
), _s1 AS (
  SELECT
    merchant_id,
    MIN(start_date) AS min_startdate
  FROM _t0
  GROUP BY
    1
), _s3 AS (
  SELECT
    merchant_id,
    start_date,
    MAX(cid) AS max_cid
  FROM main.coupons
  GROUP BY
    1,
    2
)
SELECT
  merchants.mid AS merchants_id,
  merchants.created_at AS merchant_registration_date,
  _s1.min_startdate AS earliest_coupon_start_date,
  _s3.max_cid AS earliest_coupon_id
FROM main.merchants AS merchants
LEFT JOIN _s1 AS _s1
  ON _s1.merchant_id = merchants.mid
LEFT JOIN _s3 AS _s3
  ON _s1.min_startdate = _s3.start_date AND _s3.merchant_id = merchants.mid
JOIN _t0 AS _s5
  ON _s5.merchant_id = merchants.mid
  AND _s5.start_date <= DATE_ADD(CAST(merchants.created_at AS TIMESTAMP), 1, 'YEAR')
