WITH "_T" AS (
  SELECT
    balance AS BALANCE,
    user_id AS USER_ID,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS "_W"
  FROM MAIN.WALLET_USER_BALANCE_DAILY
)
SELECT
  USER_ID AS user_id,
  BALANCE AS latest_balance
FROM "_T"
WHERE
  "_W" = 1
