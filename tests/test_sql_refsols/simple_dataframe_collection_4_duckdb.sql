SELECT
  users.user_id,
  users.country
FROM (VALUES
  (1, 'US'),
  (2, 'CR'),
  (3, 'US'),
  (4, 'MX')) AS users(user_id, country)
