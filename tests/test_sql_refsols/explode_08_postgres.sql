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
  _s2.idx - 1 AS idx3,
  _s2.val AS val3
FROM _q_0 AS _q_0
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(_q_0.comment, '.')) WITH ORDINALITY AS _s0(val, idx)
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(_s0.val, ' ')) WITH ORDINALITY AS _s1(val, idx), LATERAL UNNEST(STRING_TO_ARRAY(_s1.val, ',')) WITH ORDINALITY AS _s2(val, idx)
WHERE
  _s2.val <> ''
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST,
  3 NULLS FIRST,
  4 NULLS FIRST
