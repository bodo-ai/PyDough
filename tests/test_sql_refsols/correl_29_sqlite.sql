WITH _t3 AS (
  SELECT
    c_acctbal,
    c_nationkey
  FROM tpch.customer
), _s1 AS (
  SELECT
    AVG(c_acctbal) AS avg_cust_acctbal,
    c_nationkey
  FROM _t3
  GROUP BY
    c_nationkey
), _s3 AS (
  SELECT DISTINCT
    s_nationkey
  FROM tpch.supplier
), _t1 AS (
  SELECT
    MAX(nation.n_name) AS anything_n_name,
    MAX(nation.n_nationkey) AS anything_n_nationkey,
    MAX(nation.n_regionkey) AS anything_n_regionkey,
    COUNT(*) AS n_rows
  FROM tpch.nation AS nation
  JOIN _s1 AS _s1
    ON _s1.c_nationkey = nation.n_nationkey
  JOIN _s3 AS _s3
    ON _s3.s_nationkey = nation.n_nationkey
  JOIN _t3 AS _s5
    ON _s1.avg_cust_acctbal < _s5.c_acctbal AND _s5.c_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_nationkey
), _s7 AS (
  SELECT
    MAX(c_acctbal) AS max_c_acctbal,
    MIN(c_acctbal) AS min_c_acctbal,
    c_nationkey
  FROM _t3
  GROUP BY
    c_nationkey
), _s9 AS (
  SELECT DISTINCT
    c_nationkey
  FROM tpch.customer
), _t6 AS (
  SELECT
    s_acctbal,
    s_nationkey
  FROM tpch.supplier
), _s11 AS (
  SELECT
    AVG(s_acctbal) AS avg_supp_acctbal,
    s_nationkey
  FROM _t6
  GROUP BY
    s_nationkey
), _s15 AS (
  SELECT
    COUNT(*) AS n_rows,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN _s9 AS _s9
    ON _s9.c_nationkey = nation.n_nationkey
  JOIN _s11 AS _s11
    ON _s11.s_nationkey = nation.n_nationkey
  JOIN _t6 AS _s13
    ON _s11.avg_supp_acctbal < _s13.s_acctbal AND _s13.s_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_nationkey
)
SELECT
  _t1.anything_n_regionkey AS region_key,
  _t1.anything_n_name AS nation_name,
  _t1.n_rows AS n_above_avg_customers,
  _s15.n_rows AS n_above_avg_suppliers,
  _s7.min_c_acctbal AS min_cust_acctbal,
  _s7.max_c_acctbal AS max_cust_acctbal
FROM _t1 AS _t1
JOIN _s7 AS _s7
  ON _s7.c_nationkey = _t1.anything_n_nationkey
JOIN _s15 AS _s15
  ON _s15.n_nationkey = _t1.anything_n_nationkey
WHERE
  _t1.anything_n_regionkey IN (1, 3)
ORDER BY
  _t1.anything_n_regionkey,
  _t1.anything_n_name
