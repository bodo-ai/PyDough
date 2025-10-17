WITH _t5 AS (
  SELECT
    c_acctbal,
    c_nationkey
  FROM tpch.customer
), _s1 AS (
  SELECT
    c_nationkey,
    AVG(c_acctbal) AS avg_c_acctbal
  FROM _t5
  GROUP BY
    1
), _t3 AS (
  SELECT
    nation.n_nationkey,
    MAX(nation.n_name) AS anything_n_name,
    MAX(nation.n_regionkey) AS anything_n_regionkey,
    COUNT(*) AS n_rows
  FROM tpch.nation AS nation
  JOIN _s1 AS _s1
    ON _s1.c_nationkey = nation.n_nationkey
  JOIN _t5 AS _s3
    ON _s1.avg_c_acctbal < _s3.c_acctbal AND _s3.c_nationkey = nation.n_nationkey
  GROUP BY
    1
), _t6 AS (
  SELECT
    s_acctbal,
    s_nationkey
  FROM tpch.supplier
), _s7 AS (
  SELECT
    s_nationkey,
    AVG(s_acctbal) AS avg_s_acctbal
  FROM _t6
  GROUP BY
    1
), _t1 AS (
  SELECT
    _s5.c_nationkey,
    MAX(_t3.anything_n_name) AS anything_anything_n_name,
    MAX(_t3.anything_n_regionkey) AS anything_anything_n_regionkey,
    MAX(_t3.n_rows) AS anything_n_rows,
    MAX(_s5.c_acctbal) AS max_c_acctbal,
    MIN(_s5.c_acctbal) AS min_c_acctbal
  FROM _t3 AS _t3
  JOIN _t5 AS _s5
    ON _s5.c_nationkey = _t3.n_nationkey
  JOIN tpch.nation AS nation
    ON _s5.c_nationkey = nation.n_nationkey
  JOIN _s7 AS _s7
    ON _s7.s_nationkey = nation.n_nationkey
  JOIN _t6 AS _s9
    ON _s7.avg_s_acctbal < _s9.s_acctbal AND _s9.s_nationkey = nation.n_nationkey
  WHERE
    _t3.anything_n_regionkey IN (1, 3)
  GROUP BY
    1
)
SELECT
  MAX(anything_anything_n_regionkey) AS region_key,
  MAX(anything_anything_n_name) AS nation_name,
  MAX(anything_n_rows) AS n_above_avg_customers,
  COUNT(*) AS n_above_avg_suppliers,
  MAX(min_c_acctbal) AS min_cust_acctbal,
  MAX(max_c_acctbal) AS max_cust_acctbal
FROM _t1
GROUP BY
  c_nationkey
ORDER BY
  1,
  2
