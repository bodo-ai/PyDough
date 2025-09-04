WITH _t1 AS (
  SELECT
    c_acctbal,
    c_nationkey
  FROM tpch.customer
), _s1 AS (
  SELECT
    AVG(c_acctbal) AS avg_cust_acctbal_1,
    c_nationkey
  FROM _t1
  GROUP BY
    2
), _t2 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
  WHERE
    NOT r_name IN ('MIDDLE EAST', 'AFRICA', 'ASIA')
), _s12 AS (
  SELECT
    COUNT(*) AS n_above_avg_customers,
    MAX(nation.n_name) AS nation_name,
    MAX(LOWER(_t2.r_name)) AS region_name,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN _s1 AS _s1
    ON _s1.c_nationkey = nation.n_nationkey
  JOIN _t2 AS _t2
    ON _t2.r_regionkey = nation.n_regionkey
  JOIN _t1 AS _s5
    ON _s1.avg_cust_acctbal_1 < _s5.c_acctbal AND _s5.c_nationkey = nation.n_nationkey
  GROUP BY
    4
), _t4 AS (
  SELECT
    s_acctbal,
    s_nationkey
  FROM tpch.supplier
), _s7 AS (
  SELECT
    AVG(s_acctbal) AS avg_supp_acctbal,
    s_nationkey
  FROM _t4
  GROUP BY
    2
), _s13 AS (
  SELECT
    COUNT(*) AS n_above_avg_suppliers,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN _s7 AS _s7
    ON _s7.s_nationkey = nation.n_nationkey
  JOIN _t2 AS _t5
    ON _t5.r_regionkey = nation.n_regionkey
  JOIN _t4 AS _s11
    ON _s11.s_acctbal > _s7.avg_supp_acctbal AND _s11.s_nationkey = nation.n_nationkey
  GROUP BY
    2
)
SELECT
  _s12.region_name,
  _s12.nation_name,
  _s12.n_above_avg_customers,
  _s13.n_above_avg_suppliers
FROM _s12 AS _s12
JOIN _s13 AS _s13
  ON _s12.n_nationkey = _s13.n_nationkey
ORDER BY
  1,
  2
