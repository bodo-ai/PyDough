WITH _t2 AS (
  SELECT
    r_name AS name,
    r_regionkey AS key
  FROM tpch.region
  WHERE
    r_name = 'EUROPE'
), _t3 AS (
  SELECT
    ps_partkey AS part_key,
    ps_suppkey AS supplier_key,
    ps_supplycost AS supplycost
  FROM tpch.partsupp
), _s5 AS (
  SELECT
    MIN(supplycost) AS agg_0,
    part_key,
    supplier_key
  FROM _t3
  GROUP BY
    part_key,
    supplier_key
), _s6 AS (
  SELECT
    MIN(_s5.agg_0) AS agg_0,
    _s5.part_key
  FROM tpch.nation AS nation
  JOIN _t2 AS _t2
    ON _t2.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _s5 AS _s5
    ON _s5.supplier_key = supplier.s_suppkey
  GROUP BY
    _s5.part_key
), _s16 AS (
  SELECT
    MIN(_s6.agg_0) AS best_cost,
    part.p_partkey AS key_9
  FROM _s6 AS _s6
  JOIN tpch.part AS part
    ON _s6.part_key = part.p_partkey AND part.p_size = 15 AND part.p_type LIKE '%BRASS'
  GROUP BY
    part.p_partkey
), _s17 AS (
  SELECT
    part.p_partkey,
    part.p_mfgr,
    nation.n_name,
    supplier.s_acctbal,
    supplier.s_address,
    supplier.s_comment,
    supplier.s_name,
    supplier.s_phone,
    part.p_partkey AS key_19,
    _s13.supplycost
  FROM tpch.nation AS nation
  JOIN _t2 AS _t5
    ON _t5.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _t3 AS _s13
    ON _s13.supplier_key = supplier.s_suppkey
  JOIN tpch.part AS part
    ON _s13.part_key = part.p_partkey AND part.p_size = 15 AND part.p_type LIKE '%BRASS'
)
SELECT
  _s17.s_acctbal AS S_ACCTBAL,
  _s17.s_name AS S_NAME,
  _s17.n_name AS N_NAME,
  _s17.p_partkey AS P_PARTKEY,
  _s17.p_mfgr AS P_MFGR,
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
