SELECT
  users.user_id,
  users.signup_date
FROM (VALUES
  ROW(1, CAST('2024-01-10 00:00:00' AS DATETIME)),
  ROW(2, CAST('2024-01-12 00:00:00' AS DATETIME)),
  ROW(3, CAST('2024-02-01 00:00:00' AS DATETIME)),
  ROW(4, CAST('2024-02-01 00:00:00' AS DATETIME))) AS users(user_id, signup_date)
