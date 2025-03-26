WITH _table_alias_0 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
), _t4 AS (
  SELECT
    region.r_name AS name,
    region.r_regionkey AS key
  FROM tpch.region AS region
  WHERE
    region.r_name = 'EUROPE'
), _table_alias_1 AS (
  SELECT
    _t4.key AS key
  FROM _t4 AS _t4
), _table_alias_2 AS (
  SELECT
    _table_alias_0.key AS key
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.region_key = _table_alias_1.key
), _table_alias_3 AS (
  SELECT
    supplier.s_suppkey AS key,
    supplier.s_nationkey AS nation_key
  FROM tpch.supplier AS supplier
), _table_alias_4 AS (
  SELECT
    _table_alias_3.key AS key_5
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key = _table_alias_3.nation_key
), _table_alias_5 AS (
  SELECT
    partsupp.ps_partkey AS part_key,
    partsupp.ps_suppkey AS supplier_key,
    partsupp.ps_supplycost AS supplycost
  FROM tpch.partsupp AS partsupp
), _table_alias_6 AS (
  SELECT
    _table_alias_5.part_key AS part_key,
    _table_alias_5.supplycost AS supplycost
  FROM _table_alias_4 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.key_5 = _table_alias_5.supplier_key
), _table_alias_7 AS (
  SELECT
    part.p_partkey AS key
  FROM tpch.part AS part
  WHERE
    part.p_size = 15 AND part.p_type LIKE '%BRASS'
), _table_alias_16 AS (
  SELECT
    MIN(_table_alias_6.supplycost) AS best_cost,
    _table_alias_7.key AS key_9
  FROM _table_alias_6 AS _table_alias_6
  JOIN _table_alias_7 AS _table_alias_7
    ON _table_alias_6.part_key = _table_alias_7.key
  GROUP BY
    _table_alias_7.key
), _table_alias_8 AS (
  SELECT
    nation.n_name AS n_name,
    nation.n_nationkey AS key,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
), _table_alias_9 AS (
  SELECT
    _t6.key AS key
  FROM _t4 AS _t6
), _table_alias_10 AS (
  SELECT
    _table_alias_8.key AS key,
    _table_alias_8.n_name AS n_name
  FROM _table_alias_8 AS _table_alias_8
  JOIN _table_alias_9 AS _table_alias_9
    ON _table_alias_8.region_key = _table_alias_9.key
), _table_alias_11 AS (
  SELECT
    supplier.s_acctbal AS account_balance,
    supplier.s_address AS address,
    supplier.s_comment AS comment,
    supplier.s_suppkey AS key,
    supplier.s_name AS name,
    supplier.s_nationkey AS nation_key,
    supplier.s_phone AS phone
  FROM tpch.supplier AS supplier
), _table_alias_12 AS (
  SELECT
    _table_alias_11.key AS key_15,
    _table_alias_11.account_balance AS s_acctbal,
    _table_alias_11.address AS s_address,
    _table_alias_11.comment AS s_comment,
    _table_alias_11.name AS s_name,
    _table_alias_11.phone AS s_phone,
    _table_alias_10.n_name AS n_name
  FROM _table_alias_10 AS _table_alias_10
  JOIN _table_alias_11 AS _table_alias_11
    ON _table_alias_10.key = _table_alias_11.nation_key
), _table_alias_14 AS (
  SELECT
    _table_alias_12.n_name AS n_name,
    _table_alias_13.part_key AS part_key,
    _table_alias_12.s_acctbal AS s_acctbal,
    _table_alias_12.s_address AS s_address,
    _table_alias_12.s_comment AS s_comment,
    _table_alias_12.s_name AS s_name,
    _table_alias_12.s_phone AS s_phone,
    _table_alias_13.supplycost AS supplycost
  FROM _table_alias_12 AS _table_alias_12
  JOIN _table_alias_5 AS _table_alias_13
    ON _table_alias_12.key_15 = _table_alias_13.supplier_key
), _table_alias_15 AS (
  SELECT
    part.p_partkey AS key,
    part.p_mfgr AS manufacturer
  FROM tpch.part AS part
  WHERE
    part.p_size = 15 AND part.p_type LIKE '%BRASS'
), _table_alias_17 AS (
  SELECT
    _table_alias_15.key AS key_19,
    _table_alias_15.manufacturer AS manufacturer,
    _table_alias_14.n_name AS n_name,
    _table_alias_14.s_acctbal AS s_acctbal,
    _table_alias_14.s_address AS s_address,
    _table_alias_14.s_comment AS s_comment,
    _table_alias_14.s_name AS s_name,
    _table_alias_14.s_phone AS s_phone,
    _table_alias_14.supplycost AS supplycost
  FROM _table_alias_14 AS _table_alias_14
  JOIN _table_alias_15 AS _table_alias_15
    ON _table_alias_14.part_key = _table_alias_15.key
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
