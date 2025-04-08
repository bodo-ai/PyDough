WITH _t1 AS (
  SELECT
    r_name AS name,
    r_regionkey AS key
  FROM tpch.region
  WHERE
    r_name = 'EUROPE'
), _s6 AS (
  SELECT
    MIN(partsupp.ps_supplycost) AS agg_0,
    partsupp.ps_partkey AS part_key
  FROM tpch.nation AS nation
  JOIN _t1 AS _t1
    ON _t1.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN tpch.partsupp AS partsupp
    ON partsupp.ps_suppkey = supplier.s_suppkey
  GROUP BY
    partsupp.ps_partkey
), _s17 AS (
  SELECT
    nation.n_name,
    part.p_mfgr,
    part.p_partkey,
    supplier.s_acctbal,
    supplier.s_address,
    supplier.s_comment,
    supplier.s_name,
    supplier.s_phone,
    part.p_partkey AS key_19,
    partsupp.ps_supplycost AS supplycost
  FROM tpch.nation AS nation
  JOIN _t1 AS _t3
    ON _t3.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN tpch.partsupp AS partsupp
    ON partsupp.ps_suppkey = supplier.s_suppkey
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
    AND part.p_size = 15
    AND part.p_type LIKE '%BRASS'
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
FROM _s6 AS _s6
JOIN tpch.part AS part
  ON _s6.part_key = part.p_partkey AND part.p_size = 15 AND part.p_type LIKE '%BRASS'
JOIN _s17 AS _s17
  ON _s17.key_19 = part.p_partkey AND _s17.supplycost = _s6.agg_0
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 10
