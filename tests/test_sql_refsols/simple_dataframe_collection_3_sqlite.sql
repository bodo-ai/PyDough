SELECT
  users.column1 AS user_id,
  users.column2 AS """`name""""[""",
  users.column3 AS """space country""",
  users.column4 AS """CAST"""
FROM (VALUES
  (1, 'Alice', 'US', 25),
  (2, 'Bob', 'CR', 30),
  (3, 'Charlie', 'US', 22),
  (4, 'David', 'MX', 30)) AS users
