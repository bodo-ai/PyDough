WITH _q_0 AS (
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
    _s1.index - 1 AS idx2,
    _s2.index - 1 AS idx3,
    _q_0.key,
    _s2.value AS val3
  FROM _q_0 AS _q_0
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_q_0.comment, '.') AS _s0
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_s0.value, ',') AS _s1, LATERAL SPLIT_TO_TABLE(_s1.value, ' ') AS _s2
  WHERE
    _s2.value <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _q_0.key ORDER BY _s0.index - 1 DESC, _s1.index - 1 DESC, _s2.index - 1) = 1
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  val3
FROM _t0
