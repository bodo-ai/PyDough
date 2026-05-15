WITH _t1 AS (
  SELECT
    r_name
  FROM tpch.region
), _s0 AS (
  SELECT DISTINCT
    IFF(CONTAINS(r_name, 'A'), 'A', '*') AS a
  FROM _t1
), _s1 AS (
  SELECT DISTINCT
    IFF(CONTAINS(r_name, 'E'), 'E', '*') AS e
  FROM _t1
), _s3 AS (
  SELECT DISTINCT
    IFF(CONTAINS(r_name, 'I'), 'I', '*') AS i
  FROM _t1
), _s5 AS (
  SELECT DISTINCT
    IFF(CONTAINS(r_name, 'O'), 'O', '*') AS o
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
  1 NULLS FIRST,
  2 NULLS FIRST,
  3 NULLS FIRST,
  4 NULLS FIRST
