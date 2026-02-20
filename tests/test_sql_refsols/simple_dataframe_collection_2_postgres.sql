SELECT
  users.signup_date,
  users.user_id
FROM (VALUES
  (CAST('2024-01-10 00:00:00' AS TIMESTAMP), 1),
  (CAST('2024-01-12 00:00:00' AS TIMESTAMP), 2),
  (CAST('2024-02-01 00:00:00' AS TIMESTAMP), 3),
  (CAST('2024-02-01 00:00:00' AS TIMESTAMP), 4)) AS users(signup_date, user_id)
