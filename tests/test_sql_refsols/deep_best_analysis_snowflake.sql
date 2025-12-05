WITH _t2 AS (
  SELECT
    c_acctbal,
    c_custkey,
    c_nationkey
  FROM tpch.customer
), _t1 AS (
  SELECT
    c_acctbal,
    c_custkey,
    c_nationkey
  FROM _t2
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC, c_custkey) = 1
), _s4 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.nation
), _t3 AS (
  SELECT
    _s5.c_acctbal,
    _s4.n_nationkey,
    _s4.n_regionkey
  FROM _s4 AS _s4
  JOIN _t2 AS _s5
    ON _s4.n_nationkey = _s5.c_nationkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY _s5.c_acctbal DESC, _s5.c_custkey) = 1
), _t7 AS (
  SELECT
    _s8.n_nationkey,
    _s8.n_regionkey,
    partsupp.ps_availqty,
    partsupp.ps_partkey,
    supplier.s_suppkey
  FROM _s4 AS _s8
  JOIN tpch.supplier AS supplier
    ON _s8.n_nationkey = supplier.s_nationkey
  JOIN tpch.partsupp AS partsupp
    ON partsupp.ps_suppkey = supplier.s_suppkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY partsupp.ps_availqty DESC, partsupp.ps_partkey) = 1
), _t5 AS (
  SELECT
    n_nationkey,
    n_regionkey,
    ps_availqty,
    ps_partkey,
    s_suppkey
  FROM _t7
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY ps_availqty DESC, s_suppkey) = 1
    AND n_nationkey = s_nationkey
), _t9 AS (
  SELECT
    c_custkey,
    c_nationkey
  FROM _t2
  QUALIFY
    ROW_NUMBER() OVER (ORDER BY c_acctbal DESC, c_custkey) = 1
)
SELECT
  region.r_name,
  nation.n_name,
  _t1.c_custkey AS c_key,
  _t1.c_acctbal AS c_bal,
  _t3.c_acctbal AS cr_bal,
  _t5.s_suppkey AS s_key,
  _t5.ps_partkey AS p_key,
  _t5.ps_availqty AS p_qty,
  _t9.c_custkey AS cg_key
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN _t1 AS _t1
  ON _t1.c_nationkey = nation.n_nationkey
LEFT JOIN _t3 AS _t3
  ON _t3.n_nationkey = nation.n_nationkey AND _t3.n_regionkey = region.r_regionkey
LEFT JOIN _t5 AS _t5
  ON _t5.n_nationkey = nation.n_nationkey AND _t5.n_regionkey = region.r_regionkey
LEFT JOIN _t9 AS _t9
  ON _t9.c_nationkey = nation.n_nationkey
ORDER BY
  2 NULLS FIRST
LIMIT 10
