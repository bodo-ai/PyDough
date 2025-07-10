WITH _u_0 AS (
  SELECT
    receiver_id AS _u_1
  FROM MAIN.WALLET_TRANSACTIONS_DAILY
  WHERE
    receiver_type = 1
  GROUP BY
    receiver_id
)
SELECT
  MERCHANTS.mid AS merchant
FROM MAIN.MERCHANTS AS MERCHANTS
LEFT JOIN _u_0 AS _u_0
  ON MERCHANTS.mid = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
