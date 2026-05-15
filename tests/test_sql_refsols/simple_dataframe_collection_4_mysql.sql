SELECT
  users.user_id,
  users.country
FROM (VALUES
  ROW(1, 'US'),
  ROW(2, 'CR'),
  ROW(3, 'US'),
  ROW(4, 'MX')) AS users(user_id, country)
