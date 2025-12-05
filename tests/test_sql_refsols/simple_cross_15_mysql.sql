WITH _t1 AS (
  SELECT
    r_name
  FROM tpch.REGION
), _s0 AS (
  SELECT DISTINCT
    CASE WHEN r_name LIKE '%A%' THEN 'A' ELSE '*' END AS a
  FROM _t1
), _s1 AS (
  SELECT DISTINCT
    CASE WHEN r_name LIKE '%E%' THEN 'E' ELSE '*' END AS e
  FROM _t1
), _s3 AS (
  SELECT DISTINCT
    CASE WHEN r_name LIKE '%I%' THEN 'I' ELSE '*' END AS i
  FROM _t1
), _s5 AS (
  SELECT DISTINCT
    CASE WHEN r_name LIKE '%O%' THEN 'O' ELSE '*' END AS o
  FROM _t1
)
SELECT
  a COLLATE utf8mb4_bin AS a,
  e COLLATE utf8mb4_bin AS e,
  i COLLATE utf8mb4_bin AS i,
  o COLLATE utf8mb4_bin AS o
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
CROSS JOIN _s3 AS _s3
CROSS JOIN _s5 AS _s5
ORDER BY
  1,
  2,
  3,
  4
