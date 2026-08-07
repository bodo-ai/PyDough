WITH _q_0 AS (
  SELECT
    c_comment AS comment,
    c_custkey AS key
  FROM tpch.customer
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
)
SELECT
  _q_0.key,
  _s0.index - 1 AS idx1,
  _s1.index - 1 AS idx2,
  _s1.value AS val2
FROM _q_0 AS _q_0
CROSS JOIN LATERAL SPLIT_TO_TABLE(_q_0.comment, '.') AS _s0
CROSS JOIN LATERAL SPLIT_TO_TABLE(_s0.value, ',') AS _s1
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST,
  3 NULLS FIRST
