SELECT
  users.user_id,
  users."`name""[" AS """`name""""[""",
  users."space country" AS """space country""",
  users."CAST" AS ""CAST""
FROM (VALUES
  (1, 'Alice', 'US', 25),
  (2, 'Bob', 'CR', 30),
  (3, 'Charlie', 'US', 22),
  (4, 'David', 'MX', 30)) AS users(user_id, "`name""[", "space country", "CAST")
