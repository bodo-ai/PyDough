SELECT
  users.user_id,
  users.signup_date
FROM (VALUES
  (1, CAST('2024-01-10 00:00:00' AS TIMESTAMP)),
  (2, CAST('2024-01-12 00:00:00' AS TIMESTAMP)),
  (3, CAST('2024-02-01 00:00:00' AS TIMESTAMP)),
  (4, CAST('2024-02-01 00:00:00' AS TIMESTAMP))) AS users(user_id, signup_date)
