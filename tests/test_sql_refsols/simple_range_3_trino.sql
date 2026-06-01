SELECT
  t1.foo
FROM (VALUES
  (15),
  (16),
  (17),
  (18),
  (19)) AS t1(foo)
ORDER BY
  1 NULLS FIRST
