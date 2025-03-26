WITH _table_alias_0 AS (
  SELECT
    merchants.created_at AS created_at,
    merchants.mid AS mid
  FROM main.merchants AS merchants
), _t1 AS (
  SELECT
    coupons.merchant_id AS merchant_id,
    coupons.start_date AS start_date
  FROM main.coupons AS coupons
), _table_alias_1 AS (
  SELECT
    MIN(_t1.start_date) AS agg_0,
    _t1.merchant_id AS merchant_id
  FROM _t1 AS _t1
  GROUP BY
    _t1.merchant_id
), _table_alias_6 AS (
  SELECT
    _table_alias_1.agg_0 AS earliest_coupon_start_date,
    _table_alias_0.created_at AS merchant_registration_date,
    _table_alias_0.mid AS merchants_id,
    _table_alias_0.mid AS mid
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.mid = _table_alias_1.merchant_id
), _table_alias_2 AS (
  SELECT
    merchants.mid AS mid
  FROM main.merchants AS merchants
), _table_alias_3 AS (
  SELECT
    MIN(_t4.start_date) AS agg_0,
    _t4.merchant_id AS merchant_id
  FROM _t1 AS _t4
  GROUP BY
    _t4.merchant_id
), _table_alias_4 AS (
  SELECT
    _table_alias_3.agg_0 AS earliest_coupon_start_date,
    _table_alias_2.mid AS mid
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.mid = _table_alias_3.merchant_id
), _table_alias_5 AS (
  SELECT
    coupons.cid AS cid,
    coupons.merchant_id AS merchant_id,
    coupons.start_date AS start_date
  FROM main.coupons AS coupons
), _table_alias_7 AS (
  SELECT
    MAX(_table_alias_5.cid) AS agg_1,
    _table_alias_4.mid AS mid
  FROM _table_alias_4 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.earliest_coupon_start_date = _table_alias_5.start_date
    AND _table_alias_4.mid = _table_alias_5.merchant_id
  GROUP BY
    _table_alias_4.mid
), _table_alias_8 AS (
  SELECT
    _table_alias_6.mid AS mid,
    _table_alias_7.agg_1 AS earliest_coupon_id,
    _table_alias_6.earliest_coupon_start_date AS earliest_coupon_start_date,
    _table_alias_6.merchant_registration_date AS merchant_registration_date,
    _table_alias_6.merchants_id AS merchants_id
  FROM _table_alias_6 AS _table_alias_6
  LEFT JOIN _table_alias_7 AS _table_alias_7
    ON _table_alias_6.mid = _table_alias_7.mid
)
SELECT
  _table_alias_8.merchants_id AS merchants_id,
  _table_alias_8.merchant_registration_date AS merchant_registration_date,
  _table_alias_8.earliest_coupon_start_date AS earliest_coupon_start_date,
  _table_alias_8.earliest_coupon_id AS earliest_coupon_id
FROM _table_alias_8 AS _table_alias_8
JOIN _t1 AS _table_alias_9
  ON _table_alias_8.mid = _table_alias_9.merchant_id
  AND _table_alias_9.start_date <= DATE_ADD(CAST(_table_alias_8.merchant_registration_date AS TIMESTAMP), 1, 'YEAR')
