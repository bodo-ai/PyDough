WITH _s3 AS (
  SELECT
    MIN(start_date) AS agg_0,
    merchant_id
  FROM main.coupons
  GROUP BY
    merchant_id
), _s7 AS (
  SELECT
    MIN(start_date) AS agg_0,
    merchant_id
  FROM main.coupons
  GROUP BY
    merchant_id
), _s12 AS (
  SELECT
    MAX(_s8.cid) AS agg_1,
    _s4.mid
  FROM main.merchants AS _s4
  LEFT JOIN _s7 AS _s7
    ON _s4.mid = _s7.merchant_id
  JOIN main.coupons AS _s8
    ON _s4.mid = _s8.merchant_id AND _s7.agg_0 = _s8.start_date
  GROUP BY
    _s4.mid
)
SELECT
  _s0.mid AS merchants_id,
  _s0.created_at AS merchant_registration_date,
  _s3.agg_0 AS earliest_coupon_start_date,
  _s12.agg_1 AS earliest_coupon_id
FROM main.merchants AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.mid = _s3.merchant_id
LEFT JOIN _s12 AS _s12
  ON _s0.mid = _s12.mid
JOIN main.coupons AS _s13
  ON _s0.mid = _s13.merchant_id
WHERE
  _s13.start_date <= DATETIME(_s0.created_at, '1 year')
