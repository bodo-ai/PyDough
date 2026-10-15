WITH _s1 AS (
  SELECT
    c_comment AS comment,
    c_custkey AS key
  FROM tpch.customer
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
)
SELECT
  _s1.key,
  _s0.idx - 1 AS idx1,
  _s2.idx - 1 AS idx2,
  _s2.val AS val2
FROM _s1 AS _s1
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(_s1.comment, '.')) WITH ORDINALITY AS _s0(val, idx)
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(_s0.val, ',')) WITH ORDINALITY AS _s2(val, idx)
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST,
  3 NULLS FIRST
