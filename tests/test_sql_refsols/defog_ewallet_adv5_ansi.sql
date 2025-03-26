SELECT
  AVG(balance) AS AMB
FROM (
  SELECT
    balance
  FROM (
    SELECT
      balance,
      updated_at
    FROM main.wallet_user_balance_daily
  )
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), updated_at, DAY) <= 7
)
