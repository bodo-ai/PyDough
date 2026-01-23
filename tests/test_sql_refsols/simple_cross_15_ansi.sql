WITH _t1 AS (
  SELECT
    r_name
  FROM tpch.region
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
  _s0.a,
  _s1.e,
  _s3.i,
  _s5.o
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
CROSS JOIN _s3 AS _s3
CROSS JOIN _s5 AS _s5
ORDER BY
  1,
  2,
  3,
  4
