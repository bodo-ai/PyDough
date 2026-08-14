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
  _s0.idx - 1 AS idx1,
  _s1.idx - 1 AS idx2,
  _s1.val AS val2
FROM _q_0 AS _q_0
CROSS JOIN UNNEST(SPLIT(_q_0.comment, '.')) WITH ORDINALITY AS _s0(val, idx)
CROSS JOIN UNNEST(SPLIT(_s0.val, ',')) WITH ORDINALITY AS _s1(val, idx)
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST,
  3 NULLS FIRST
