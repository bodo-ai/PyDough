SELECT
  users.user_id,
  users.```name"[` AS `"``name""["`,
  users.`space country` AS `"space country"`,
  users.`CAST` AS "CAST"
FROM (VALUES
  ROW(1, 'Alice', 'US', 25),
  ROW(2, 'Bob', 'CR', 30),
  ROW(3, 'Charlie', 'US', 22),
  ROW(4, 'David', 'MX', 30)) AS users(user_id, ```name"[`, `space country`, `CAST`)
