WITH _t3 AS (
  SELECT
    r_name AS name,
    r_regionkey AS key
  FROM tpch.region
  WHERE
    r_name = 'EUROPE'
), _s5 AS (
  SELECT
    ps_partkey AS part_key,
    ps_suppkey AS supplier_key,
    ps_supplycost AS supplycost
  FROM tpch.partsupp
), _s16 AS (
  SELECT
    MIN(_s5.supplycost) AS best_cost,
    part.p_partkey AS key_9
  FROM tpch.nation AS nation
  JOIN _t3 AS _t3
    ON _t3.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _s5 AS _s5
    ON _s5.supplier_key = supplier.s_suppkey
  JOIN tpch.part AS part
    ON _s5.part_key = part.p_partkey AND part.p_size = 15 AND part.p_type LIKE '%BRASS'
  GROUP BY
    part.p_partkey
), _s17 AS (
  SELECT
    part.p_partkey AS key_19,
    part.p_mfgr AS manufacturer,
    nation.n_name,
    supplier.s_acctbal,
    supplier.s_address,
    supplier.s_comment,
    supplier.s_name,
    supplier.s_phone,
    _s13.supplycost
  FROM tpch.nation AS nation
  JOIN _t3 AS _t5
    ON _t5.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _s5 AS _s13
    ON _s13.supplier_key = supplier.s_suppkey
  JOIN tpch.part AS part
    ON _s13.part_key = part.p_partkey AND part.p_size = 15 AND part.p_type LIKE '%BRASS'
)
SELECT
  _s17.s_acctbal AS S_ACCTBAL,
  _s17.s_name AS S_NAME,
  _s17.n_name AS N_NAME,
  _s17.key_19 AS P_PARTKEY,
  _s17.manufacturer AS P_MFGR,
  _s17.s_address AS S_ADDRESS,
  _s17.s_phone AS S_PHONE,
  _s17.s_comment AS S_COMMENT
FROM _s16 AS _s16
JOIN _s17 AS _s17
  ON _s16.best_cost = _s17.supplycost AND _s16.key_9 = _s17.key_19
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 10
