WITH _t1 AS (
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
), _table_alias_3 AS (
  SELECT
    MIN(_t4.start_date) AS agg_0,
    _t4.merchant_id AS merchant_id
  FROM _t1 AS _t4
  GROUP BY
    _t4.merchant_id
), _table_alias_7 AS (
  SELECT
    MAX(coupons.cid) AS agg_1,
    merchants.mid AS mid
  FROM main.merchants AS merchants
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_3.merchant_id = merchants.mid
  JOIN main.coupons AS coupons
    ON _table_alias_3.agg_0 = coupons.start_date AND coupons.merchant_id = merchants.mid
  GROUP BY
    merchants.mid
)
SELECT
  merchants.mid AS merchants_id,
  merchants.created_at AS merchant_registration_date,
  _table_alias_1.agg_0 AS earliest_coupon_start_date,
  _table_alias_7.agg_1 AS earliest_coupon_id
FROM main.merchants AS merchants
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.merchant_id = merchants.mid
LEFT JOIN _table_alias_7 AS _table_alias_7
  ON _table_alias_7.mid = merchants.mid
JOIN _t1 AS _table_alias_9
  ON _table_alias_9.merchant_id = merchants.mid
  AND _table_alias_9.start_date <= DATE_ADD(CAST(merchants.created_at AS TIMESTAMP), 1, 'YEAR')
