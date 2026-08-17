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
  _s0.idx AS idx1,
  _s1.idx AS idx2,
  _s2.idx AS idx3,
  _s2.val AS val3
FROM _q_0 AS _q_0
CROSS JOIN LATERAL (
  SELECT
    UNNEST(STR_SPLIT(_q_0.comment, '.')) AS _col_0,
    GENERATE_SUBSCRIPTS(STR_SPLIT(_q_0.comment, '.'), 1) - 1 AS _col_1
) AS _s0(val, idx)
CROSS JOIN LATERAL (
  SELECT
    UNNEST(STR_SPLIT(_s0.val, ' ')) AS _col_0,
    GENERATE_SUBSCRIPTS(STR_SPLIT(_s0.val, ' '), 1) - 1 AS _col_1
) AS _s1(val, idx), LATERAL (
  SELECT
    UNNEST(STR_SPLIT(_s1.val, ',')) AS _col_0,
    GENERATE_SUBSCRIPTS(STR_SPLIT(_s1.val, ','), 1) - 1 AS _col_1
) AS _s2(val, idx)
WHERE
  _s2.val <> ''
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST,
  3 NULLS FIRST,
  4 NULLS FIRST
