SELECT
  USERS.USER_ID AS user_id,
  USERS."`name'[",
  USERS."space country",
  USERS."CAST"
FROM (VALUES
  (1, 'Alice', 'US', 25),
  (2, 'Bob', 'CR', 30),
  (3, 'Charlie', 'US', 22),
  (4, 'David', 'MX', 30)) AS USERS(USER_ID, "`name'[", "space country", "CAST")
