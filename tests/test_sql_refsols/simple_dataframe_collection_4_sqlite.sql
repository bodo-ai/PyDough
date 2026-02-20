SELECT
  users.column1 AS user_id,
  users.column2 AS country
FROM (VALUES
  (1, 'US'),
  (2, 'CR'),
  (3, 'US'),
  (4, 'MX')) AS users
