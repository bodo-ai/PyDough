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
FROM ewallet.wallet_transactions_daily AS wallet_transactions_daily
JOIN ewallet.users AS users
  ON users.status = 'active' AND users.uid = wallet_transactions_daily.sender_id
WHERE
  EXTRACT(YEAR FROM CAST(wallet_transactions_daily.created_at AS DATETIME)) = 2023
  AND wallet_transactions_daily.sender_type = 0
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  1
