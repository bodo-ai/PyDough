WITH _t0 AS (
  SELECT
    part.p_mfgr,
    part.p_partkey,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.part AS part
  JOIN tpch.partsupp AS partsupp
    ON part.p_partkey = partsupp.ps_partkey
  JOIN tpch.supplier AS supplier
    ON partsupp.ps_suppkey = supplier.s_suppkey
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
  WHERE
    part.p_size = 15 AND part.p_type LIKE '%BRASS'
  QUALIFY
    RANK() OVER (PARTITION BY partsupp.ps_partkey ORDER BY partsupp.ps_supplycost NULLS LAST) = 1
)
SELECT
  supplier.s_acctbal AS S_ACCTBAL,
  supplier.s_name AS S_NAME,
  nation.n_name AS N_NAME,
  _t0.p_partkey AS P_PARTKEY,
  _t0.p_mfgr AS P_MFGR,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  supplier.s_comment AS S_COMMENT
FROM _t0 AS _t0
JOIN tpch.supplier AS supplier
  ON _t0.supplier_key = supplier.s_suppkey
JOIN tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 10
