WITH _t4 AS (
  SELECT
    region.r_name AS name,
    region.r_regionkey AS key
  FROM tpch.region AS region
  WHERE
    region.r_name = 'EUROPE'
), _table_alias_5 AS (
  SELECT
    partsupp.ps_partkey AS part_key,
    partsupp.ps_suppkey AS supplier_key,
    partsupp.ps_supplycost AS supplycost
  FROM tpch.partsupp AS partsupp
), _table_alias_16 AS (
  SELECT
    MIN(_table_alias_5.supplycost) AS best_cost,
    part.p_partkey AS key_9
  FROM tpch.nation AS nation
  JOIN _t4 AS _t4
    ON _t4.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_5.supplier_key = supplier.s_suppkey
  JOIN tpch.part AS part
    ON _table_alias_5.part_key = part.p_partkey
    AND part.p_size = 15
    AND part.p_type LIKE '%BRASS'
  GROUP BY
    part.p_partkey
), _table_alias_17 AS (
  SELECT
    part.p_partkey AS key_19,
    part.p_mfgr AS manufacturer,
    nation.n_name AS n_name,
    supplier.s_acctbal AS s_acctbal,
    supplier.s_address AS s_address,
    supplier.s_comment AS s_comment,
    supplier.s_name AS s_name,
    supplier.s_phone AS s_phone,
    _table_alias_13.supplycost AS supplycost
  FROM tpch.nation AS nation
  JOIN _t4 AS _t6
    ON _t6.key = nation.n_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _table_alias_5 AS _table_alias_13
    ON _table_alias_13.supplier_key = supplier.s_suppkey
  JOIN tpch.part AS part
    ON _table_alias_13.part_key = part.p_partkey
    AND part.p_size = 15
    AND part.p_type LIKE '%BRASS'
), _t0 AS (
  SELECT
    _table_alias_17.n_name AS n_name,
    _table_alias_17.manufacturer AS p_mfgr,
    _table_alias_17.key_19 AS p_partkey,
    _table_alias_17.s_acctbal AS s_acctbal,
    _table_alias_17.s_address AS s_address,
    _table_alias_17.s_comment AS s_comment,
    _table_alias_17.s_name AS s_name,
    _table_alias_17.s_phone AS s_phone,
    _table_alias_17.s_acctbal AS ordering_1,
    _table_alias_17.n_name AS ordering_2,
    _table_alias_17.s_name AS ordering_3,
    _table_alias_17.key_19 AS ordering_4
  FROM _table_alias_16 AS _table_alias_16
  JOIN _table_alias_17 AS _table_alias_17
    ON _table_alias_16.best_cost = _table_alias_17.supplycost
    AND _table_alias_16.key_9 = _table_alias_17.key_19
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3,
    ordering_4
  LIMIT 10
)
SELECT
  _t0.s_acctbal AS S_ACCTBAL,
  _t0.s_name AS S_NAME,
  _t0.n_name AS N_NAME,
  _t0.p_partkey AS P_PARTKEY,
  _t0.p_mfgr AS P_MFGR,
  _t0.s_address AS S_ADDRESS,
  _t0.s_phone AS S_PHONE,
  _t0.s_comment AS S_COMMENT
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2,
  _t0.ordering_3,
  _t0.ordering_4
