WITH _t2 AS (
  SELECT
    c_acctbal,
    c_nationkey
  FROM tpch.customer
), _s1 AS (
  SELECT
    AVG(c_acctbal) AS avg_cust_acctbal,
    c_nationkey
  FROM _t2
  GROUP BY
    c_nationkey
), _s3 AS (
  SELECT DISTINCT
    s_nationkey
  FROM tpch.supplier
), _t3 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
  WHERE
    NOT r_name IN ('MIDDLE EAST', 'AFRICA', 'ASIA')
), _s16 AS (
  SELECT
    ANY_VALUE(nation.n_name) AS anything_n_name,
    ANY_VALUE(LOWER(_t3.r_name)) AS anything_region_name,
    COUNT(*) AS n_rows,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN _s1 AS _s1
    ON _s1.c_nationkey = nation.n_nationkey
  JOIN _s3 AS _s3
    ON _s3.s_nationkey = nation.n_nationkey
  JOIN _t3 AS _t3
    ON _t3.r_regionkey = nation.n_regionkey
  JOIN _t2 AS _s7
    ON _s1.avg_cust_acctbal < _s7.c_acctbal AND _s7.c_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_nationkey
), _s9 AS (
  SELECT DISTINCT
    c_nationkey
  FROM tpch.customer
), _t5 AS (
  SELECT
    s_acctbal,
    s_nationkey
  FROM tpch.supplier
), _s11 AS (
  SELECT
    AVG(s_acctbal) AS avg_supp_acctbal,
    s_nationkey
  FROM _t5
  GROUP BY
    s_nationkey
), _s17 AS (
  SELECT
    COUNT(*) AS n_rows,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN _s9 AS _s9
    ON _s9.c_nationkey = nation.n_nationkey
  JOIN _s11 AS _s11
    ON _s11.s_nationkey = nation.n_nationkey
  JOIN _t3 AS _t6
    ON _t6.r_regionkey = nation.n_regionkey
  JOIN _t5 AS _s15
    ON _s11.avg_supp_acctbal < _s15.s_acctbal AND _s15.s_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_nationkey
)
SELECT
  _s16.anything_region_name AS region_name,
  _s16.anything_n_name AS nation_name,
  _s16.n_rows AS n_above_avg_customers,
  _s17.n_rows AS n_above_avg_suppliers
FROM _s16 AS _s16
JOIN _s17 AS _s17
  ON _s16.n_nationkey = _s17.n_nationkey
ORDER BY
  _s16.anything_region_name,
  _s16.anything_n_name
