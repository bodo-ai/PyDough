WITH _s0 AS (
  SELECT
    s_nationkey,
    s_suppkey
  FROM tpch.supplier
), _t4 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
  WHERE
    n_name = 'GERMANY'
), _s8 AS (
  SELECT
    COALESCE(SUM(partsupp.ps_supplycost * partsupp.ps_availqty), 0) * 0.0001 AS min_market_share
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s0
    ON _s0.s_suppkey = partsupp.ps_suppkey
  JOIN _t4 AS _t4
    ON _s0.s_nationkey = _t4.n_nationkey
), _s9 AS (
  SELECT
    COALESCE(SUM(partsupp.ps_supplycost * partsupp.ps_availqty), 0) AS value,
    partsupp.ps_partkey
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s4
    ON _s4.s_suppkey = partsupp.ps_suppkey
  JOIN _t4 AS _t8
    ON _s4.s_nationkey = _t8.n_nationkey
  GROUP BY
    partsupp.ps_partkey
)
SELECT
  _s9.ps_partkey AS PS_PARTKEY,
  _s9.value AS VALUE
FROM _s8 AS _s8
JOIN _s9 AS _s9
  ON _s8.min_market_share < _s9.value
ORDER BY
  value DESC
LIMIT 10
