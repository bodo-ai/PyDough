WITH _t1 AS (
  SELECT
    merchant_id,
    start_date
  FROM main.coupons
), _s1 AS (
  SELECT
    MIN(start_date) AS min_start_date,
    merchant_id
  FROM _t1
  GROUP BY
    merchant_id
), _s3 AS (
  SELECT
    MIN(start_date) AS min_start_date,
    merchant_id
  FROM _t1
  GROUP BY
    merchant_id
), _s7 AS (
  SELECT
    MAX(coupons.cid) AS max_cid,
    merchants.mid
  FROM main.merchants AS merchants
  LEFT JOIN _s3 AS _s3
    ON _s3.merchant_id = merchants.mid
  JOIN main.coupons AS coupons
    ON _s3.min_start_date = coupons.start_date AND coupons.merchant_id = merchants.mid
  GROUP BY
    merchants.mid
)
SELECT
  merchants.mid AS merchants_id,
  merchants.created_at AS merchant_registration_date,
  _s1.min_start_date AS earliest_coupon_start_date,
  _s7.max_cid AS earliest_coupon_id
FROM main.merchants AS merchants
LEFT JOIN _s1 AS _s1
  ON _s1.merchant_id = merchants.mid
LEFT JOIN _s7 AS _s7
  ON _s7.mid = merchants.mid
JOIN _t1 AS _s9
  ON _s9.merchant_id = merchants.mid
  AND _s9.start_date <= DATE_ADD(CAST(merchants.created_at AS DATETIME), INTERVAL '1' YEAR)
