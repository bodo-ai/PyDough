WITH _t1 AS (
  SELECT
    merchant_id,
    start_date
  FROM main.coupons
), _t1_2 AS (
  SELECT
    MIN(start_date) AS agg_0,
    merchant_id
  FROM _t1
  GROUP BY
    merchant_id
), _t3 AS (
  SELECT
    MIN(start_date) AS agg_0,
    merchant_id
  FROM _t1
  GROUP BY
    merchant_id
), _t7 AS (
  SELECT
    MAX(coupons.cid) AS agg_1,
    merchants.mid
  FROM main.merchants AS merchants
  LEFT JOIN _t3 AS _t3
    ON _t3.merchant_id = merchants.mid
  JOIN main.coupons AS coupons
    ON _t3.agg_0 = coupons.start_date AND coupons.merchant_id = merchants.mid
  GROUP BY
    merchants.mid
)
SELECT
  merchants.mid AS merchants_id,
  merchants.created_at AS merchant_registration_date,
  _t1.agg_0 AS earliest_coupon_start_date,
  _t7.agg_1 AS earliest_coupon_id
FROM main.merchants AS merchants
LEFT JOIN _t1_2 AS _t1
  ON _t1.merchant_id = merchants.mid
LEFT JOIN _t7 AS _t7
  ON _t7.mid = merchants.mid
JOIN _t1 AS _t9
  ON _t9.merchant_id = merchants.mid
  AND _t9.start_date <= DATETIME(merchants.created_at, '1 year')
