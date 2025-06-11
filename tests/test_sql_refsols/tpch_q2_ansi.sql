WITH _s5 AS (
  SELECT
    nation.n_name AS expr_8,
    region.r_regionkey AS key
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
), _s7 AS (
  SELECT
    supplier.s_acctbal AS account_balance,
    supplier.s_address AS address,
    supplier.s_comment AS comment,
    _s5.expr_8,
    _s5.key,
    supplier.s_name AS name,
    supplier.s_phone AS phone
  FROM tpch.supplier AS supplier
  JOIN _s5 AS _s5
    ON _s5.key = supplier.s_nationkey
), _t1 AS (
  SELECT
    _s7.comment AS comment_8,
    _s7.name AS name_10,
    part.p_mfgr,
    part.p_partkey,
    _s7.account_balance,
    _s7.address,
    _s7.expr_8,
    _s7.phone
  FROM tpch.part AS part
  JOIN tpch.partsupp AS partsupp
    ON part.p_partkey = partsupp.ps_partkey
  JOIN _s7 AS _s7
    ON _s7.key = partsupp.ps_suppkey
  WHERE
    part.p_size = 15 AND part.p_type LIKE '%BRASS'
  QUALIFY
    RANK() OVER (PARTITION BY partsupp.ps_partkey ORDER BY partsupp.ps_supplycost NULLS LAST) = 1
)
SELECT
  account_balance AS S_ACCTBAL,
  name_10 AS S_NAME,
  expr_8 AS N_NAME,
  p_partkey AS P_PARTKEY,
  p_mfgr AS P_MFGR,
  address AS S_ADDRESS,
  phone AS S_PHONE,
  comment_8 AS S_COMMENT
FROM _t1
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 10
