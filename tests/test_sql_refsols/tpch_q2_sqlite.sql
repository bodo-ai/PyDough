WITH _t3 AS (
  SELECT
    r_name AS name,
    r_regionkey AS key
  FROM tpch.region
  WHERE
    r_name = 'EUROPE'
), _s6 AS (
  SELECT
    MIN(partsupp.ps_supplycost) AS best_cost,
    partsupp.ps_partkey AS part_key
  FROM tpch.nation AS nation
  JOIN _t3 AS _t3
    ON _t3.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN tpch.partsupp AS partsupp
    ON partsupp.ps_suppkey = supplier.s_suppkey
  GROUP BY
    partsupp.ps_partkey
), _s17 AS (
  SELECT
    part.p_partkey AS key_17,
    part.p_mfgr AS manufacturer,
    nation.n_name,
    supplier.s_acctbal,
    supplier.s_address,
    supplier.s_comment,
    supplier.s_name,
    supplier.s_phone,
    partsupp.ps_supplycost AS supply_cost
  FROM tpch.nation AS nation
  JOIN _t3 AS _t5
    ON _t5.key = nation.n_regionkey
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
  _s17.key_17 AS P_PARTKEY,
  _s17.manufacturer AS P_MFGR,
  _s17.s_address AS S_ADDRESS,
  _s17.s_phone AS S_PHONE,
  _s17.s_comment AS S_COMMENT
FROM _s6 AS _s6
JOIN tpch.part AS part
  ON _s6.part_key = part.p_partkey AND part.p_size = 15 AND part.p_type LIKE '%BRASS'
JOIN _s17 AS _s17
  ON _s17.key_17 = part.p_partkey AND _s17.supply_cost = _s6.best_cost
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 10
