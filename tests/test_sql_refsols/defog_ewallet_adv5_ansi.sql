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
  ) AS _t1
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), updated_at, DAY) <= 7
) AS _t0
