SELECT
  users.column1 AS signup_date,
  users.column2 AS user_id
FROM (VALUES
  ('2024-01-10 00:00:00', 1),
  ('2024-01-12 00:00:00', 2),
  ('2024-02-01 00:00:00', 3),
  ('2024-02-01 00:00:00', 4)) AS users
