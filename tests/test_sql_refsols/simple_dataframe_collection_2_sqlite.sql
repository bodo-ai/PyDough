SELECT
  users.column1 AS user_id,
  users.column2 AS signup_date
FROM (VALUES
  (1, '2024-01-10 00:00:00'),
  (2, '2024-01-12 00:00:00'),
  (3, '2024-02-01 00:00:00'),
  (4, '2024-02-01 00:00:00')) AS users
