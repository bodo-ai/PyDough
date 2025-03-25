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
    CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(updated_at, 'start of day'))) AS INTEGER) <= 7
) AS _t0
