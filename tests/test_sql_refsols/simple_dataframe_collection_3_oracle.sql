SELECT
  COLUMN1 AS user_id,
  COLUMN2 AS "`name""[",
  COLUMN3 AS "space country",
  COLUMN4 AS "CAST"
FROM (VALUES
  (1, 'Alice', 'US', 25),
  (2, 'Bob', 'CR', 30),
  (3, 'Charlie', 'US', 22),
  (4, 'David', 'MX', 30)) AS USERS(USER_ID, "`name""[", "space country", "CAST")
