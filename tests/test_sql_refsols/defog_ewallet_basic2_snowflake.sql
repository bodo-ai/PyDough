WITH _u_0 AS (
  SELECT
    merchant_id AS _u_1
  FROM MAIN.COUPONS
  GROUP BY
    1
)
SELECT
  MERCHANTS.mid AS merchant_id,
  MERCHANTS.name AS merchant_name
FROM MAIN.MERCHANTS AS MERCHANTS
LEFT JOIN _u_0 AS _u_0
  ON MERCHANTS.mid = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
