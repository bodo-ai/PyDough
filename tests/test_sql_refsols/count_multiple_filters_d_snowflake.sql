WITH _t1 AS (
  SELECT
    1 AS "_"
  FROM tpch.customer
  QUALIFY
    NTILE(100) OVER (ORDER BY c_acctbal) = 100
), _s2 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _t1
), _s0 AS (
  SELECT
    c_acctbal,
    c_nationkey
  FROM tpch.customer
), _t6 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
  WHERE
    n_name = 'GERMANY'
), _t4 AS (
  SELECT
    1 AS "_"
  FROM _s0 AS _s0
  JOIN _t6 AS _t6
    ON _s0.c_nationkey = _t6.n_nationkey
  QUALIFY
    NTILE(100) OVER (ORDER BY _s0.c_acctbal) = 100
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _t4
), _s4 AS (
  SELECT
    c_nationkey
  FROM tpch.customer
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _s4 AS _s4
  JOIN _t6 AS _t8
    ON _s4.c_nationkey = _t8.n_nationkey
), _t12 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
  WHERE
    n_name = 'CHINA'
), _t10 AS (
  SELECT
    1 AS "_"
  FROM _s0 AS _s8
  JOIN _t12 AS _t12
    ON _s8.c_nationkey = _t12.n_nationkey
  QUALIFY
    NTILE(100) OVER (ORDER BY _s8.c_acctbal) = 100
), _s11 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _t10
), _t14 AS (
  SELECT
    1 AS "_"
  FROM _s0 AS _s12
  JOIN tpch.nation AS nation
    ON _s12.c_nationkey = nation.n_nationkey
  QUALIFY
    NTILE(100) OVER (ORDER BY _s12.c_acctbal) = 100 AND nation.n_name = 'CHINA'
), _s15 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _t14
), _s19 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _s4 AS _s16
  JOIN _t12 AS _t17
    ON _s16.c_nationkey = _t17.n_nationkey
)
SELECT
  _s2.n_rows AS n1,
  _s3.n_rows AS n2,
  _s7.n_rows AS n3,
  _s11.n_rows AS n4,
  _s15.n_rows AS n5,
  _s19.n_rows AS n6
FROM _s2 AS _s2
CROSS JOIN _s3 AS _s3
CROSS JOIN _s7 AS _s7
CROSS JOIN _s11 AS _s11
CROSS JOIN _s15 AS _s15
CROSS JOIN _s19 AS _s19
