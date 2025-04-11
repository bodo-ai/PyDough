SELECT
  10 AS s1,
  CASE WHEN 20 <= 10 THEN 20 WHEN 10 <= 20 THEN 10 END AS s2,
  CASE WHEN 20 <= 20 THEN 20 WHEN 20 <= 20 THEN 20 END AS s3,
  CASE
    WHEN 20 <= 10 AND 20 <= 0
    THEN 20
    WHEN 10 <= 20 AND 10 <= 0
    THEN 10
    WHEN 0 <= 20 AND 0 <= 10
    THEN 0
  END AS s4,
  CASE
    WHEN 20 <= 10 AND 20 <= 10 AND 20 <= -1 AND 20 <= -2 AND 20 <= 100 AND 20 <= -200
    THEN 20
    WHEN 10 <= 20 AND 10 <= 10 AND 10 <= -1 AND 10 <= -2 AND 10 <= 100 AND 10 <= -200
    THEN 10
    WHEN 10 <= 20 AND 10 <= 10 AND 10 <= -1 AND 10 <= -2 AND 10 <= 100 AND 10 <= -200
    THEN 10
    WHEN -1 <= 20 AND -1 <= 10 AND -1 <= 10 AND -1 <= -2 AND -1 <= 100 AND -1 <= -200
    THEN -1
    WHEN -2 <= 20 AND -2 <= 10 AND -2 <= 10 AND -2 <= -1 AND -2 <= 100 AND -2 <= -200
    THEN -2
    WHEN 100 <= 20 AND 100 <= 10 AND 100 <= 10 AND 100 <= -1 AND 100 <= -2 AND 100 <= -200
    THEN 100
    WHEN -200 <= 20
    AND -200 <= 10
    AND -200 <= 10
    AND -200 <= -1
    AND -200 <= -2
    AND -200 <= 100
    THEN -200
  END AS s5,
  CASE
    WHEN 20 <= 10 AND 20 <= NULL AND 20 <= 100 AND 20 <= 200
    THEN 20
    WHEN 10 <= 20 AND 10 <= NULL AND 10 <= 100 AND 10 <= 200
    THEN 10
    WHEN NULL <= 20 AND NULL <= 10 AND NULL <= 100 AND NULL <= 200
    THEN NULL
    WHEN 100 <= 20 AND 100 <= 10 AND 100 <= NULL AND 100 <= 200
    THEN 100
    WHEN 200 <= 20 AND 200 <= 10 AND 200 <= NULL AND 200 <= 100
    THEN 200
  END AS s6,
  CASE
    WHEN 20.22 <= 10.22 AND 20.22 <= -0.34
    THEN 20.22
    WHEN 10.22 <= 20.22 AND 10.22 <= -0.34
    THEN 10.22
    WHEN -0.34 <= 20.22 AND -0.34 <= 10.22
    THEN -0.34
  END AS s7,
  CASE
    WHEN CAST('2025-01-01 00:00:00' AS TIMESTAMP) <= CAST('2024-01-01 00:00:00' AS TIMESTAMP)
    AND CAST('2025-01-01 00:00:00' AS TIMESTAMP) <= CAST('2023-01-01 00:00:00' AS TIMESTAMP)
    THEN CAST('2025-01-01 00:00:00' AS TIMESTAMP)
    WHEN CAST('2024-01-01 00:00:00' AS TIMESTAMP) <= CAST('2025-01-01 00:00:00' AS TIMESTAMP)
    AND CAST('2024-01-01 00:00:00' AS TIMESTAMP) <= CAST('2023-01-01 00:00:00' AS TIMESTAMP)
    THEN CAST('2024-01-01 00:00:00' AS TIMESTAMP)
    WHEN CAST('2023-01-01 00:00:00' AS TIMESTAMP) <= CAST('2025-01-01 00:00:00' AS TIMESTAMP)
    AND CAST('2023-01-01 00:00:00' AS TIMESTAMP) <= CAST('2024-01-01 00:00:00' AS TIMESTAMP)
    THEN CAST('2023-01-01 00:00:00' AS TIMESTAMP)
  END AS s8,
  CASE
    WHEN '' <= 'alphabet soup' AND '' <= 'Hello World'
    THEN ''
    WHEN 'alphabet soup' <= '' AND 'alphabet soup' <= 'Hello World'
    THEN 'alphabet soup'
    WHEN 'Hello World' <= '' AND 'Hello World' <= 'alphabet soup'
    THEN 'Hello World'
  END AS s9,
  CASE
    WHEN NULL <= 'alphabet soup' AND NULL <= 'Hello World'
    THEN NULL
    WHEN 'alphabet soup' <= NULL AND 'alphabet soup' <= 'Hello World'
    THEN 'alphabet soup'
    WHEN 'Hello World' <= NULL AND 'Hello World' <= 'alphabet soup'
    THEN 'Hello World'
  END AS s10,
  10 AS l1,
  CASE WHEN 20 >= 10 THEN 20 WHEN 10 >= 20 THEN 10 END AS l2,
  CASE WHEN 20 >= 20 THEN 20 WHEN 20 >= 20 THEN 20 END AS l3,
  CASE
    WHEN 20 >= 10 AND 20 >= 0
    THEN 20
    WHEN 10 >= 20 AND 10 >= 0
    THEN 10
    WHEN 0 >= 20 AND 0 >= 10
    THEN 0
  END AS l4,
  CASE
    WHEN 20 >= 10
    AND 20 >= 10
    AND 20 >= -1
    AND 20 >= -2
    AND 20 >= 100
    AND 20 >= -200
    AND 20 >= 300
    THEN 20
    WHEN 10 >= 20
    AND 10 >= 10
    AND 10 >= -1
    AND 10 >= -2
    AND 10 >= 100
    AND 10 >= -200
    AND 10 >= 300
    THEN 10
    WHEN 10 >= 20
    AND 10 >= 10
    AND 10 >= -1
    AND 10 >= -2
    AND 10 >= 100
    AND 10 >= -200
    AND 10 >= 300
    THEN 10
    WHEN -1 >= 20
    AND -1 >= 10
    AND -1 >= 10
    AND -1 >= -2
    AND -1 >= 100
    AND -1 >= -200
    AND -1 >= 300
    THEN -1
    WHEN -2 >= 20
    AND -2 >= 10
    AND -2 >= 10
    AND -2 >= -1
    AND -2 >= 100
    AND -2 >= -200
    AND -2 >= 300
    THEN -2
    WHEN 100 >= 20
    AND 100 >= 10
    AND 100 >= 10
    AND 100 >= -1
    AND 100 >= -2
    AND 100 >= -200
    AND 100 >= 300
    THEN 100
    WHEN -200 >= 20
    AND -200 >= 10
    AND -200 >= 10
    AND -200 >= -1
    AND -200 >= -2
    AND -200 >= 100
    AND -200 >= 300
    THEN -200
    WHEN 300 >= 20
    AND 300 >= 10
    AND 300 >= 10
    AND 300 >= -1
    AND 300 >= -2
    AND 300 >= 100
    AND 300 >= -200
    THEN 300
  END AS l5,
  CASE
    WHEN 20 >= 10 AND 20 >= NULL AND 20 >= 100 AND 20 >= 200
    THEN 20
    WHEN 10 >= 20 AND 10 >= NULL AND 10 >= 100 AND 10 >= 200
    THEN 10
    WHEN NULL >= 20 AND NULL >= 10 AND NULL >= 100 AND NULL >= 200
    THEN NULL
    WHEN 100 >= 20 AND 100 >= 10 AND 100 >= NULL AND 100 >= 200
    THEN 100
    WHEN 200 >= 20 AND 200 >= 10 AND 200 >= NULL AND 200 >= 100
    THEN 200
  END AS l6,
  CASE
    WHEN 20.22 >= 100.22 AND 20.22 >= -0.34
    THEN 20.22
    WHEN 100.22 >= 20.22 AND 100.22 >= -0.34
    THEN 100.22
    WHEN -0.34 >= 20.22 AND -0.34 >= 100.22
    THEN -0.34
  END AS l7,
  CASE
    WHEN CAST('2025-01-01 00:00:00' AS TIMESTAMP) >= CAST('2024-01-01 00:00:00' AS TIMESTAMP)
    AND CAST('2025-01-01 00:00:00' AS TIMESTAMP) >= CAST('2023-01-01 00:00:00' AS TIMESTAMP)
    THEN CAST('2025-01-01 00:00:00' AS TIMESTAMP)
    WHEN CAST('2024-01-01 00:00:00' AS TIMESTAMP) >= CAST('2025-01-01 00:00:00' AS TIMESTAMP)
    AND CAST('2024-01-01 00:00:00' AS TIMESTAMP) >= CAST('2023-01-01 00:00:00' AS TIMESTAMP)
    THEN CAST('2024-01-01 00:00:00' AS TIMESTAMP)
    WHEN CAST('2023-01-01 00:00:00' AS TIMESTAMP) >= CAST('2025-01-01 00:00:00' AS TIMESTAMP)
    AND CAST('2023-01-01 00:00:00' AS TIMESTAMP) >= CAST('2024-01-01 00:00:00' AS TIMESTAMP)
    THEN CAST('2023-01-01 00:00:00' AS TIMESTAMP)
  END AS l8,
  CASE
    WHEN '' >= 'alphabet soup' AND '' >= 'Hello World'
    THEN ''
    WHEN 'alphabet soup' >= '' AND 'alphabet soup' >= 'Hello World'
    THEN 'alphabet soup'
    WHEN 'Hello World' >= '' AND 'Hello World' >= 'alphabet soup'
    THEN 'Hello World'
  END AS l9,
  CASE
    WHEN NULL >= 'alphabet soup' AND NULL >= 'Hello World'
    THEN NULL
    WHEN 'alphabet soup' >= NULL AND 'alphabet soup' >= 'Hello World'
    THEN 'alphabet soup'
    WHEN 'Hello World' >= NULL AND 'Hello World' >= 'alphabet soup'
    THEN 'Hello World'
  END AS l10
FROM (
  SELECT
    *
  FROM (VALUES
    (NULL))
)
