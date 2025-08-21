WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    sender_id
  FROM main.wallet_transactions_daily
  WHERE
    sender_type = 0
  GROUP BY
    2
)
SELECT
  users.uid AS user_id,
  _s1.n_rows AS total_transactions
FROM main.users AS users
JOIN _s1 AS _s1
  ON _s1.sender_id = users.uid
