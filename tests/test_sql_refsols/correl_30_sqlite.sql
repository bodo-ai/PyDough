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
    2
), _t3 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
  WHERE
    NOT r_name IN ('MIDDLE EAST', 'AFRICA', 'ASIA')
), _t5 AS (
  SELECT
    s_acctbal,
    s_nationkey
  FROM tpch.supplier
), _s7 AS (
  SELECT
    AVG(s_acctbal) AS avg_supp_acctbal,
    s_nationkey
  FROM _t5
  GROUP BY
    2
), _s13 AS (
  SELECT
    COUNT(*) AS n_rows,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN _s7 AS _s7
    ON _s7.s_nationkey = nation.n_nationkey
  JOIN _t3 AS _t6
    ON _t6.r_regionkey = nation.n_regionkey
  JOIN _t5 AS _s11
    ON _s11.s_acctbal > _s7.avg_supp_acctbal AND _s11.s_nationkey = nation.n_nationkey
  GROUP BY
    2
)
SELECT
  MAX(LOWER(_t3.r_name)) AS region_name,
  MAX(nation.n_name) AS nation_name,
  COUNT(*) AS n_above_avg_customers,
  MAX(_s13.n_rows) AS n_above_avg_suppliers
FROM tpch.nation AS nation
JOIN _s1 AS _s1
  ON _s1.c_nationkey = nation.n_nationkey
JOIN _t3 AS _t3
  ON _t3.r_regionkey = nation.n_regionkey
JOIN _t2 AS _s5
  ON _s1.avg_cust_acctbal < _s5.c_acctbal AND _s5.c_nationkey = nation.n_nationkey
JOIN _s13 AS _s13
  ON _s13.n_nationkey = anything_n_nationkey
GROUP BY
  nation.n_nationkey,
  n_nationkey
ORDER BY
  1,
  2
