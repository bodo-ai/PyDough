WITH _u_0 AS (
  SELECT
    merchant_id AS _u_1
  FROM cassandra.defog.coupons
  GROUP BY
    1
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name
FROM mongo.defog.merchants AS merchants
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = merchants.mid
WHERE
  _u_0._u_1 IS NULL
