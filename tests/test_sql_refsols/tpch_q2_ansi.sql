WITH _t1 AS (
  SELECT
    _s4.s_comment AS comment_8,
    _s4.s_name AS name_10,
    _s0.p_mfgr,
    _s0.p_partkey,
    _s4.s_acctbal AS account_balance,
    _s4.s_address AS address,
    _s5.n_name AS expr_8,
    _s4.s_phone AS phone
  FROM tpch.part AS _s0
  JOIN tpch.partsupp AS _s1
    ON _s0.p_partkey = _s1.ps_partkey
  JOIN tpch.supplier AS _s4
    ON _s1.ps_suppkey = _s4.s_suppkey
  JOIN tpch.nation AS _s5
    ON _s4.s_nationkey = _s5.n_nationkey
  JOIN tpch.region AS _s6
    ON _s5.n_regionkey = _s6.r_regionkey AND _s6.r_name = 'EUROPE'
  WHERE
    _s0.p_size = 15 AND _s0.p_type LIKE '%BRASS'
  QUALIFY
    RANK() OVER (PARTITION BY _s1.ps_partkey ORDER BY _s1.ps_supplycost NULLS LAST) = 1
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
