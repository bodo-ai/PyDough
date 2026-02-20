SELECT
  COLUMN1 AS user_id,
  COLUMN2 AS country
FROM (VALUES
  (1, 'US'),
  (2, 'CR'),
  (3, 'US'),
  (4, 'MX')) AS USERS(USER_ID, COUNTRY)
