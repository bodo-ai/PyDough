WITH _s0 AS (
  SELECT
    s_nationkey,
    s_suppkey
  FROM tpch.SUPPLIER
), _t4 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.NATION
  WHERE
    n_name = 'GERMANY'
), _s8 AS (
  SELECT
    COALESCE(SUM(PARTSUPP.ps_supplycost * PARTSUPP.ps_availqty), 0) * 0.0001 AS min_market_share
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN _s0 AS _s0
    ON PARTSUPP.ps_suppkey = _s0.s_suppkey
  JOIN _t4 AS _t4
    ON _s0.s_nationkey = _t4.n_nationkey
), _s9 AS (
  SELECT
    COALESCE(SUM(PARTSUPP.ps_supplycost * PARTSUPP.ps_availqty), 0) AS VALUE,
    PARTSUPP.ps_partkey
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN _s0 AS _s4
    ON PARTSUPP.ps_suppkey = _s4.s_suppkey
  JOIN _t4 AS _t8
    ON _s4.s_nationkey = _t8.n_nationkey
  GROUP BY
    PARTSUPP.ps_partkey
)
SELECT
  _s9.ps_partkey AS PS_PARTKEY,
  _s9.VALUE
FROM _s8 AS _s8
JOIN _s9 AS _s9
  ON _s8.min_market_share < _s9.VALUE
ORDER BY
  VALUE DESC
LIMIT 10
