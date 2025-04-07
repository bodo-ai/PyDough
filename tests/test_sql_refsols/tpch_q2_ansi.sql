WITH _t3 AS (
  SELECT
    r_name AS name,
    r_regionkey AS key
  FROM tpch.region
  WHERE
    r_name = 'EUROPE'
), _t5 AS (
  SELECT
    ps_partkey AS part_key,
    ps_suppkey AS supplier_key,
    ps_supplycost AS supplycost
  FROM tpch.partsupp
), _t16 AS (
  SELECT
    MIN(_t5.supplycost) AS best_cost,
    part.p_partkey AS key_9
  FROM tpch.nation AS nation
  JOIN _t3 AS _t3_2
    ON _t3_2.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _t5 AS _t5
    ON _t5.supplier_key = supplier.s_suppkey
  JOIN tpch.part AS part
    ON _t5.part_key = part.p_partkey AND part.p_size = 15 AND part.p_type LIKE '%BRASS'
  GROUP BY
    part.p_partkey
), _t17 AS (
  SELECT
    part.p_partkey AS key_19,
    part.p_mfgr AS manufacturer,
    nation.n_name,
    supplier.s_acctbal,
    supplier.s_address,
    supplier.s_comment,
    supplier.s_name,
    supplier.s_phone,
    _t13.supplycost
  FROM tpch.nation AS nation
  JOIN _t3 AS _t5
    ON _t5.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _t5 AS _t13
    ON _t13.supplier_key = supplier.s_suppkey
  JOIN tpch.part AS part
    ON _t13.part_key = part.p_partkey AND part.p_size = 15 AND part.p_type LIKE '%BRASS'
)
SELECT
  _t17.s_acctbal AS S_ACCTBAL,
  _t17.s_name AS S_NAME,
  _t17.n_name AS N_NAME,
  _t17.key_19 AS P_PARTKEY,
  _t17.manufacturer AS P_MFGR,
  _t17.s_address AS S_ADDRESS,
  _t17.s_phone AS S_PHONE,
  _t17.s_comment AS S_COMMENT
FROM _t16 AS _t16
JOIN _t17 AS _t17
  ON _t16.best_cost = _t17.supplycost AND _t16.key_9 = _t17.key_19
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 10
