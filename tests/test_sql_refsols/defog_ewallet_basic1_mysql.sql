SELECT
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(wallet_transactions_daily.created_at AS DATETIME)),
      ' ',
      MONTH(CAST(wallet_transactions_daily.created_at AS DATETIME)),
      ' 1'
    ),
    '%Y %c %e'
  ) AS month,
  COUNT(DISTINCT wallet_transactions_daily.sender_id) AS active_users
FROM main.wallet_transactions_daily AS wallet_transactions_daily
JOIN main.users AS users
  ON users.status = 'active' AND users.uid = wallet_transactions_daily.sender_id
WHERE
  YEAR(wallet_transactions_daily.created_at) = 2023
  AND wallet_transactions_daily.sender_type = 0
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(wallet_transactions_daily.created_at AS DATETIME)),
      ' ',
      MONTH(CAST(wallet_transactions_daily.created_at AS DATETIME)),
      ' 1'
    ),
    '%Y %c %e'
  )
