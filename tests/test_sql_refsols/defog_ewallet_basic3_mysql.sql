WITH _u_0 AS (
  SELECT
    receiver_id AS _u_1
  FROM main.wallet_transactions_daily
  WHERE
    receiver_type = 1
  GROUP BY
    1
)
SELECT
  merchants.mid AS merchant
FROM main.merchants AS merchants
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = merchants.mid
WHERE
  NOT _u_0._u_1 IS NULL
