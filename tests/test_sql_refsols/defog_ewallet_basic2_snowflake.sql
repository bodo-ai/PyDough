WITH _u_0 AS (
  SELECT
    merchant_id AS _u_1
  FROM ewallet.coupons
  GROUP BY
    1
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name
FROM ewallet.merchants AS merchants
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = merchants.mid
WHERE
  _u_0._u_1 IS NULL
