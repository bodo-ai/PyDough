WITH _s2 AS (
  SELECT
    s_suppkey AS key,
    s_nationkey AS nation_key
  FROM tpch.supplier
), _t AS (
  SELECT
    part.p_mfgr,
    part.p_partkey,
    partsupp.ps_suppkey AS supplier_key,
    RANK() OVER (PARTITION BY part.p_partkey ORDER BY partsupp.ps_supplycost) AS _w
  FROM tpch.part AS part
  JOIN tpch.partsupp AS partsupp
    ON part.p_partkey = partsupp.ps_partkey
  JOIN _s2 AS _s2
    ON _s2.key = partsupp.ps_suppkey
  JOIN tpch.nation AS nation
    ON _s2.nation_key = nation.n_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
  WHERE
    part.p_size = 15 AND part.p_type LIKE '%BRASS'
)
SELECT
  supplier.s_acctbal AS S_ACCTBAL,
  supplier.s_name AS S_NAME,
  nation.n_name AS N_NAME,
  _t.p_partkey AS P_PARTKEY,
  _t.p_mfgr AS P_MFGR,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  supplier.s_comment AS S_COMMENT
FROM _t AS _t
JOIN tpch.supplier AS supplier
  ON _t.supplier_key = supplier.s_suppkey
JOIN _s2 AS _s10
  ON _s10.key = _t.supplier_key
JOIN tpch.nation AS nation
  ON _s10.nation_key = nation.n_nationkey
WHERE
  _t._w = 1
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 10
