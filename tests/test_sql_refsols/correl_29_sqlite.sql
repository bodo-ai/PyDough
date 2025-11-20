WITH _t3 AS (
  SELECT
    c_acctbal,
    c_nationkey
  FROM tpch.customer
), _s1 AS (
  SELECT
    c_nationkey,
    AVG(c_acctbal) AS avg_cacctbal
  FROM _t3
  GROUP BY
    1
), _t1 AS (
  SELECT
    nation.n_nationkey,
    MAX(nation.n_name) AS anything_nname,
    MAX(nation.n_regionkey) AS anything_nregionkey,
    COUNT(*) AS n_rows
  FROM tpch.nation AS nation
  JOIN _s1 AS _s1
    ON _s1.c_nationkey = nation.n_nationkey
  JOIN _t3 AS _s3
    ON _s1.avg_cacctbal < _s3.c_acctbal AND _s3.c_nationkey = nation.n_nationkey
  GROUP BY
    1
), _s5 AS (
  SELECT
    c_nationkey,
    MAX(c_acctbal) AS max_cacctbal,
    MIN(c_acctbal) AS min_cacctbal
  FROM _t3
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
    AVG(s_acctbal) AS avg_sacctbal
  FROM _t6
  GROUP BY
    1
), _s11 AS (
  SELECT
    nation.n_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.nation AS nation
  JOIN _s7 AS _s7
    ON _s7.s_nationkey = nation.n_nationkey
  JOIN _t6 AS _s9
    ON _s7.avg_sacctbal < _s9.s_acctbal AND _s9.s_nationkey = nation.n_nationkey
  GROUP BY
    1
)
SELECT
  _t1.anything_nregionkey AS region_key,
  _t1.anything_nname AS nation_name,
  _t1.n_rows AS n_above_avg_customers,
  _s11.n_rows AS n_above_avg_suppliers,
  _s5.min_cacctbal AS min_cust_acctbal,
  _s5.max_cacctbal AS max_cust_acctbal
FROM _t1 AS _t1
JOIN _s5 AS _s5
  ON _s5.c_nationkey = _t1.n_nationkey
JOIN _s11 AS _s11
  ON _s11.n_nationkey = _t1.n_nationkey
WHERE
  _t1.anything_nregionkey IN (1, 3)
ORDER BY
  1,
  2
