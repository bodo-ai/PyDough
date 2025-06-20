SELECT
  MIN(20, 10) AS s1,
  MIN(20, 20) AS s2,
  MIN(20, 10, 0) AS s3,
  MIN(20, 10, 10, -1, -2, 100, -200) AS s4,
  MIN(20, 10, NULL, 100, 200) AS s5,
  MIN(20.22, 10.22, -0.34) AS s6,
  MIN('2025-01-01 00:00:00', '2024-01-01 00:00:00', '2023-01-01 00:00:00') AS s7,
  MIN('', 'alphabet soup', 'Hello World') AS s8,
  MIN(NULL, 'alphabet soup', 'Hello World') AS s9,
  MAX(20, 10) AS l1,
  MAX(20, 20) AS l2,
  MAX(20, 10, 0) AS l3,
  MAX(20, 10, 10, -1, -2, 100, -200, 300) AS l4,
  MAX(20, 10, NULL, 100, 200) AS l5,
  MAX(20.22, 100.22, -0.34) AS l6,
  MAX('2025-01-01 00:00:00', '2024-01-01 00:00:00', '2023-01-01 00:00:00') AS l7,
  MAX('', 'alphabet soup', 'Hello World') AS l8,
  MAX(NULL, 'alphabet soup', 'Hello World') AS l9
FROM (VALUES
  (NULL)) AS _q_0
