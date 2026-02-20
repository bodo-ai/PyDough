SELECT
  users.signup_date,
  users.user_id
FROM (VALUES
  ROW(CAST('2024-01-10 00:00:00' AS DATETIME), 1),
  ROW(CAST('2024-01-12 00:00:00' AS DATETIME), 2),
  ROW(CAST('2024-02-01 00:00:00' AS DATETIME), 3),
  ROW(CAST('2024-02-01 00:00:00' AS DATETIME), 4)) AS users(signup_date, user_id)
