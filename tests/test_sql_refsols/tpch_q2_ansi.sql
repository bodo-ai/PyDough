WITH _t1 AS (
  SELECT
    nation.n_name,
    part.p_mfgr,
    part.p_partkey,
    supplier.s_acctbal,
    supplier.s_address,
    supplier.s_comment,
    supplier.s_name,
    supplier.s_phone
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
    RANK() OVER (PARTITION BY ps_partkey ORDER BY partsupp.ps_supplycost NULLS LAST) = 1
)
SELECT
  s_acctbal AS S_ACCTBAL,
  s_name AS S_NAME,
  n_name AS N_NAME,
  p_partkey AS P_PARTKEY,
  p_mfgr AS P_MFGR,
  s_address AS S_ADDRESS,
  s_phone AS S_PHONE,
  s_comment AS S_COMMENT
FROM _t1
ORDER BY
  1 DESC,
  3,
  2,
  4
LIMIT 10
