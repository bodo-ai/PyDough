WITH _t AS (
  SELECT
    NATION.n_name,
    PART.p_mfgr,
    PART.p_partkey,
    SUPPLIER.s_acctbal,
    SUPPLIER.s_address,
    SUPPLIER.s_comment,
    SUPPLIER.s_name,
    SUPPLIER.s_phone,
    RANK() OVER (PARTITION BY PARTSUPP.ps_partkey ORDER BY CASE WHEN PARTSUPP.ps_supplycost IS NULL THEN 1 ELSE 0 END, PARTSUPP.ps_supplycost) AS _w
  FROM tpch.PART AS PART
  JOIN tpch.PARTSUPP AS PARTSUPP
    ON PART.p_partkey = PARTSUPP.ps_partkey
  JOIN tpch.SUPPLIER AS SUPPLIER
    ON PARTSUPP.ps_suppkey = SUPPLIER.s_suppkey
  JOIN tpch.NATION AS NATION
    ON NATION.n_nationkey = SUPPLIER.s_nationkey
  JOIN tpch.REGION AS REGION
    ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'EUROPE'
  WHERE
    PART.p_size = 15 AND PART.p_type LIKE '%BRASS'
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
FROM _t
WHERE
  _w = 1
ORDER BY
  s_acctbal DESC,
  n_name COLLATE utf8mb4_bin,
  s_name COLLATE utf8mb4_bin,
  p_partkey
LIMIT 10
