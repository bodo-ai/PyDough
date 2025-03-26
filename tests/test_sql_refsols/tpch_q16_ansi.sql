WITH _table_alias_0 AS (
  SELECT
    part.p_brand AS p_brand,
    part.p_type AS p_type,
    part.p_size AS p_size,
    part.p_partkey AS key
  FROM tpch.part AS part
  WHERE
    NOT part.p_type LIKE 'MEDIUM POLISHED%%'
    AND part.p_brand <> 'BRAND#45'
    AND part.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
), _table_alias_1 AS (
  SELECT
    partsupp.ps_partkey AS part_key,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS partsupp
), _table_alias_2 AS (
  SELECT
    _table_alias_0.p_brand AS p_brand,
    _table_alias_0.p_size AS p_size,
    _table_alias_0.p_type AS p_type,
    _table_alias_1.supplier_key AS supplier_key
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.key = _table_alias_1.part_key
), _table_alias_3 AS (
  SELECT
    supplier.s_comment AS comment,
    supplier.s_suppkey AS key
  FROM tpch.supplier AS supplier
), _t1 AS (
  SELECT
    COUNT(DISTINCT _table_alias_2.supplier_key) AS supplier_count,
    COUNT(DISTINCT _table_alias_2.supplier_key) AS ordering_1,
    _table_alias_2.p_brand AS p_brand,
    _table_alias_2.p_brand AS ordering_2,
    _table_alias_2.p_size AS p_size,
    _table_alias_2.p_size AS ordering_4,
    _table_alias_2.p_type AS p_type,
    _table_alias_2.p_type AS ordering_3
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.supplier_key = _table_alias_3.key
  WHERE
    NOT _table_alias_3.comment LIKE '%Customer%Complaints%'
  GROUP BY
    _table_alias_2.p_brand,
    _table_alias_2.p_size,
    _table_alias_2.p_type
), _t0 AS (
  SELECT
    _t1.p_brand AS p_brand,
    _t1.p_size AS p_size,
    _t1.p_type AS p_type,
    _t1.supplier_count AS supplier_count,
    _t1.ordering_1 AS ordering_1,
    _t1.ordering_2 AS ordering_2,
    _t1.ordering_3 AS ordering_3,
    _t1.ordering_4 AS ordering_4
  FROM _t1 AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3,
    ordering_4
  LIMIT 10
)
SELECT
  _t0.p_brand AS P_BRAND,
  _t0.p_type AS P_TYPE,
  _t0.p_size AS P_SIZE,
  _t0.supplier_count AS SUPPLIER_COUNT
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2,
  _t0.ordering_3,
  _t0.ordering_4
