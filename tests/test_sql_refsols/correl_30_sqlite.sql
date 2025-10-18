WITH _t2 AS (
  SELECT
    c_acctbal,
    c_nationkey
  FROM tpch.customer
), _s1 AS (
  SELECT
    c_nationkey,
    AVG(c_acctbal) AS avg_c_acctbal
  FROM _t2
  GROUP BY
    1
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
    s_nationkey,
    AVG(s_acctbal) AS avg_s_acctbal
  FROM _t5
  GROUP BY
    1
), _s13 AS (
  SELECT
    nation.n_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.nation AS nation
  JOIN _s7 AS _s7
    ON _s7.s_nationkey = nation.n_nationkey
  JOIN _t3 AS _t6
    ON _t6.r_regionkey = nation.n_regionkey
  JOIN _t5 AS _s11
    ON _s11.s_acctbal > _s7.avg_s_acctbal AND _s11.s_nationkey = nation.n_nationkey
  GROUP BY
    1
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
  ON _s1.avg_c_acctbal < _s5.c_acctbal AND _s5.c_nationkey = nation.n_nationkey
JOIN _s13 AS _s13
  ON _s13.n_nationkey = nation.n_nationkey
GROUP BY
  nation.n_nationkey
ORDER BY
  1,
  2
