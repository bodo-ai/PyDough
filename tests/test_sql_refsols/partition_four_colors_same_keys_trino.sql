WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM mysql.tpch.part
  WHERE
    p_name LIKE '%smoke%'
  GROUP BY
    p_brand
), _s0 AS (
  SELECT
    MAX(n_rows) AS max_n_rows
  FROM _t0
), _t3 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM mysql.tpch.part
  WHERE
    p_name LIKE '%rose%'
  GROUP BY
    p_brand
), _s1 AS (
  SELECT
    MAX(n_rows) AS max_n_rows
  FROM _t3
), _t6 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM mysql.tpch.part
  WHERE
    p_name LIKE '%turquoise%'
  GROUP BY
    p_brand
), _s3 AS (
  SELECT
    MAX(n_rows) AS max_n_rows
  FROM _t6
), _t9 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM mysql.tpch.part
  WHERE
    p_name LIKE '%spring%'
  GROUP BY
    p_brand
), _s5 AS (
  SELECT
    MAX(n_rows) AS max_n_rows
  FROM _t9
)
SELECT
  _s0.max_n_rows AS max_smoke,
  _s1.max_n_rows AS max_rose,
  _s3.max_n_rows AS max_turquoise,
  _s5.max_n_rows AS max_spring
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
CROSS JOIN _s3 AS _s3
CROSS JOIN _s5 AS _s5
