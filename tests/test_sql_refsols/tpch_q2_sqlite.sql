WITH _t AS (
  SELECT
    part.p_mfgr,
    part.p_partkey,
    supplier.s_acctbal AS account_balance,
    supplier.s_address AS address,
    supplier.s_comment AS comment,
    nation.n_name,
    supplier.s_name AS name,
    supplier.s_phone AS phone,
    RANK() OVER (PARTITION BY partsupp.ps_partkey ORDER BY partsupp.ps_supplycost) AS _w
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
)
SELECT
  account_balance AS S_ACCTBAL,
  name AS S_NAME,
  n_name AS N_NAME,
  p_partkey AS P_PARTKEY,
  p_mfgr AS P_MFGR,
  address AS S_ADDRESS,
  phone AS S_PHONE,
  comment AS S_COMMENT
FROM _t
WHERE
  _w = 1
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 10
