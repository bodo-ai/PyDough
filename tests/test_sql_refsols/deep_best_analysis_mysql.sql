WITH _t2 AS (
  SELECT
    c_acctbal,
    c_custkey,
    c_nationkey
  FROM tpch.CUSTOMER
), _t AS (
  SELECT
    c_acctbal,
    c_custkey,
    c_nationkey,
    ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END DESC, c_acctbal DESC, CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS _w
  FROM _t2
), _s4 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.NATION
), _t_2 AS (
  SELECT
    _s5.c_acctbal,
    _s4.n_nationkey,
    _s4.n_regionkey,
    ROW_NUMBER() OVER (PARTITION BY _s4.n_regionkey ORDER BY CASE WHEN _s5.c_acctbal IS NULL THEN 1 ELSE 0 END DESC, _s5.c_acctbal DESC, CASE WHEN _s5.c_custkey IS NULL THEN 1 ELSE 0 END, _s5.c_custkey) AS _w
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
    PARTSUPP.ps_availqty,
    PARTSUPP.ps_partkey,
    SUPPLIER.s_nationkey,
    SUPPLIER.s_suppkey,
    ROW_NUMBER() OVER (PARTITION BY _s8.n_regionkey ORDER BY CASE WHEN PARTSUPP.ps_availqty IS NULL THEN 1 ELSE 0 END DESC, PARTSUPP.ps_availqty DESC, CASE WHEN PARTSUPP.ps_partkey IS NULL THEN 1 ELSE 0 END, PARTSUPP.ps_partkey) AS _w
  FROM _s4 AS _s8
  JOIN tpch.SUPPLIER AS SUPPLIER
    ON SUPPLIER.s_nationkey = _s8.n_nationkey
  JOIN tpch.PARTSUPP AS PARTSUPP
    ON PARTSUPP.ps_suppkey = SUPPLIER.s_suppkey
), _t_4 AS (
  SELECT
    n_nationkey,
    n_regionkey,
    ps_availqty,
    ps_partkey,
    s_nationkey,
    s_suppkey,
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY CASE WHEN ps_availqty IS NULL THEN 1 ELSE 0 END DESC, ps_availqty DESC, CASE WHEN s_suppkey IS NULL THEN 1 ELSE 0 END, s_suppkey) AS _w
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
    ROW_NUMBER() OVER (ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END DESC, c_acctbal DESC, CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS _w
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
  REGION.r_name,
  NATION.n_name COLLATE utf8mb4_bin AS n_name,
  _t.c_custkey AS c_key,
  _t.c_acctbal AS c_bal,
  _s7.c_acctbal AS cr_bal,
  _s13.s_suppkey AS s_key,
  _s13.ps_partkey AS p_key,
  _s13.ps_availqty AS p_qty,
  _s15.c_custkey AS cg_key
FROM tpch.REGION AS REGION
JOIN tpch.NATION AS NATION
  ON NATION.n_regionkey = REGION.r_regionkey
JOIN _t AS _t
  ON NATION.n_nationkey = _t.c_nationkey AND _t._w = 1
LEFT JOIN _s7 AS _s7
  ON NATION.n_nationkey = _s7.n_nationkey AND REGION.r_regionkey = _s7.n_regionkey
LEFT JOIN _s13 AS _s13
  ON NATION.n_nationkey = _s13.n_nationkey AND REGION.r_regionkey = _s13.n_regionkey
LEFT JOIN _s15 AS _s15
  ON NATION.n_nationkey = _s15.c_nationkey
ORDER BY
  2
LIMIT 10
