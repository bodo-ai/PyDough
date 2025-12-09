WITH _t2 AS (
  SELECT
    c_acctbal,
    c_custkey,
    c_nationkey
  FROM tpch.customer
), _t AS (
  SELECT
    c_acctbal,
    c_custkey,
    c_nationkey,
    ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC, c_custkey) AS _w
  FROM _t2
), _s4 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.nation
), _t_2 AS (
  SELECT
    _s5.c_acctbal,
    _s4.n_nationkey,
    _s4.n_regionkey,
    ROW_NUMBER() OVER (PARTITION BY _s4.n_regionkey ORDER BY _s5.c_acctbal DESC, _s5.c_custkey) AS _w
  FROM _s4 AS _s4
  JOIN _t2 AS _s5
    ON _s4.n_nationkey = _s5.c_nationkey
), _s7 AS (
  SELECT
    c_acctbal,
    n_nationkey,
    n_regionkey
  FROM _t_2
  WHERE
    _w = 1
), _t_3 AS (
  SELECT
    _s8.n_nationkey,
    _s8.n_regionkey,
    partsupp.ps_availqty,
    partsupp.ps_partkey,
    supplier.s_nationkey,
    supplier.s_suppkey,
    ROW_NUMBER() OVER (PARTITION BY _s8.n_regionkey ORDER BY partsupp.ps_availqty DESC, partsupp.ps_partkey) AS _w
  FROM _s4 AS _s8
  JOIN tpch.supplier AS supplier
    ON _s8.n_nationkey = supplier.s_nationkey
  JOIN tpch.partsupp AS partsupp
    ON partsupp.ps_suppkey = supplier.s_suppkey
), _t_4 AS (
  SELECT
    n_nationkey,
    n_regionkey,
    ps_availqty,
    ps_partkey,
    s_nationkey,
    s_suppkey,
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY ps_availqty DESC, s_suppkey) AS _w
  FROM _t_3
  WHERE
    _w = 1
), _s13 AS (
  SELECT
    n_nationkey,
    n_regionkey,
    ps_availqty,
    ps_partkey,
    s_suppkey
  FROM _t_4
  WHERE
    _w = 1 AND n_nationkey = s_nationkey
), _t_5 AS (
  SELECT
    c_custkey,
    c_nationkey,
    ROW_NUMBER() OVER (ORDER BY c_acctbal DESC, c_custkey) AS _w
  FROM _t2
), _s15 AS (
  SELECT
    c_custkey,
    c_nationkey
  FROM _t_5
  WHERE
    _w = 1
)
SELECT
  region.r_name,
  nation.n_name,
  _t.c_custkey AS c_key,
  _t.c_acctbal AS c_bal,
  _s7.c_acctbal AS cr_bal,
  _s13.s_suppkey AS s_key,
  _s13.ps_partkey AS p_key,
  _s13.ps_availqty AS p_qty,
  _s15.c_custkey AS cg_key
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN _t AS _t
  ON _t._w = 1 AND _t.c_nationkey = nation.n_nationkey
LEFT JOIN _s7 AS _s7
  ON _s7.n_nationkey = nation.n_nationkey AND _s7.n_regionkey = region.r_regionkey
LEFT JOIN _s13 AS _s13
  ON _s13.n_nationkey = nation.n_nationkey AND _s13.n_regionkey = region.r_regionkey
LEFT JOIN _s15 AS _s15
  ON _s15.c_nationkey = nation.n_nationkey
ORDER BY
  2 NULLS FIRST
LIMIT 10
