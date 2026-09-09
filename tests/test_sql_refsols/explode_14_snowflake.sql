WITH _s1 AS (
  SELECT
    c_comment AS comment,
    c_custkey AS key
  FROM tpch.customer
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
), _t0 AS (
  SELECT
    _s0.index - 1 AS idx1,
    _s2.index - 1 AS idx2,
    _s4.index - 1 AS idx3,
    _s1.key,
    _s4.value AS val3
  FROM _s1 AS _s1
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_s1.comment, '.') AS _s0
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_s0.value, ',') AS _s2, LATERAL SPLIT_TO_TABLE(_s2.value, ' ') AS _s4
  WHERE
    _s4.value <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s1.key ORDER BY _s0.index - 1, _s2.index - 1, _s4.index - 1) = 1
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  val3
FROM _t0
