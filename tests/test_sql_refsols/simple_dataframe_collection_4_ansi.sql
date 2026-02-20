SELECT
  column1 AS user_id,
  column2 AS country
FROM (VALUES
  (1, 'US'),
  (2, 'CR'),
  (3, 'US'),
  (4, 'MX')) AS users(user_id, country)
