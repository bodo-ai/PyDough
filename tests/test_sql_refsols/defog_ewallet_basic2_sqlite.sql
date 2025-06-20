WITH _u_0 AS (
  SELECT
    coupons.merchant_id AS _u_1
  FROM main.coupons AS coupons
  GROUP BY
    coupons.merchant_id
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name
FROM main.merchants AS merchants
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = merchants.mid
WHERE
  _u_0._u_1 IS NULL
