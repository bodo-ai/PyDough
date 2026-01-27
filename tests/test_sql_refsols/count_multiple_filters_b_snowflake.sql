WITH _s0 AS (
  SELECT
    SUM(IFF(c_mktsegment = 'BUILDING', 1, 0)) AS agg_6,
    SUM(IFF(STARTSWITH(c_phone, '11'), 1, 0)) AS agg_7,
    SUM(IFF(STARTSWITH(c_phone, '11') AND c_mktsegment = 'BUILDING', 1, 0)) AS agg_9,
    COUNT(*) AS n_rows
  FROM tpch.customer
  WHERE
    c_acctbal <= 600 AND c_acctbal >= 500
), _s1 AS (
  SELECT
    SUM(IFF(STARTSWITH(c_phone, '11'), 1, 0)) AS agg_8,
    COUNT(*) AS n_rows
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
)
SELECT
  _s0.n_rows AS n1,
  _s1.n_rows AS n2,
  _s0.agg_6 AS n3,
  _s0.agg_7 AS n4,
  _s1.agg_8 AS n5,
  _s0.agg_9 AS n6
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
