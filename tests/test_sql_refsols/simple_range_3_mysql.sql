SELECT
  T1.foo
FROM VALUES
  (ROW(15)),
  (ROW(16)),
  (ROW(17)),
  (ROW(18)),
  (ROW(19)) AS T1(foo)
ORDER BY
  1
