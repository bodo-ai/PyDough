WITH _t1 AS (
  SELECT
    merchant_id,
    start_date
  FROM main.coupons
), _s1 AS (
  SELECT
    merchant_id,
    MIN(start_date) AS min_start_date
  FROM _t1
  GROUP BY
    1
), _s3 AS (
  SELECT
    cid,
    merchant_id,
    start_date
  FROM main.coupons
), _s4 AS (
  SELECT
    _s3.merchant_id,
    _s3.start_date,
    MAX(merchants.created_at) AS anything_created_at,
    MAX(_s3.cid) AS max_cid
  FROM main.merchants AS merchants
  LEFT JOIN _s1 AS _s1
    ON _s1.merchant_id = merchants.mid
  LEFT JOIN _s3 AS _s3
    ON _s1.min_start_date = _s3.start_date AND _s3.merchant_id = merchants.mid
  GROUP BY
    1,
    2
)
SELECT
  _s4.merchant_id AS merchants_id,
  _s4.anything_created_at AS merchant_registration_date,
  _s4.start_date AS earliest_coupon_start_date,
  _s4.max_cid AS earliest_coupon_id
FROM _s4 AS _s4
JOIN _t1 AS _s5
  ON _s4.merchant_id = _s5.merchant_id
  AND _s5.start_date <= CAST(_s4.anything_created_at AS TIMESTAMP) + INTERVAL '1 YEAR'
