WITH _s0 AS (
  SELECT
    c_acctbal
  FROM tpch.customer
), _s1 AS (
  SELECT
    MIN(c_acctbal) AS min_c_acctbal
  FROM _s0
), _s4 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.c_acctbal <= (
      _s1.min_c_acctbal + 10.0
    )
), _s2 AS (
  SELECT
    s_acctbal
  FROM tpch.supplier
), _s3 AS (
  SELECT
    MAX(s_acctbal) AS max_s_acctbal
  FROM _s2
), _s5 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _s2 AS _s2
  JOIN _s3 AS _s3
    ON _s2.s_acctbal >= (
      _s3.max_s_acctbal - 10.0
    )
)
SELECT
  _s4.n_rows AS n1,
  _s5.n_rows AS n2
FROM _s4 AS _s4
CROSS JOIN _s5 AS _s5
