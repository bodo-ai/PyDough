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
  _s0.idx AS idx1,
  _s2.idx AS idx2,
  _s4.idx AS idx3,
  _s4.val AS val3
FROM _s1 AS _s1
CROSS JOIN LATERAL (
  SELECT
    UNNEST(STR_SPLIT(_s1.comment, '.')) AS _col_0,
    GENERATE_SUBSCRIPTS(STR_SPLIT(_s1.comment, '.'), 1) - 1 AS _col_1
) AS _s0(val, idx)
CROSS JOIN LATERAL (
  SELECT
    UNNEST(STR_SPLIT(_s0.val, ' ')) AS _col_0,
    GENERATE_SUBSCRIPTS(STR_SPLIT(_s0.val, ' '), 1) - 1 AS _col_1
) AS _s2(val, idx), LATERAL (
  SELECT
    UNNEST(STR_SPLIT(_s2.val, ',')) AS _col_0,
    GENERATE_SUBSCRIPTS(STR_SPLIT(_s2.val, ','), 1) - 1 AS _col_1
) AS _s4(val, idx)
WHERE
  _s4.val <> ''
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST,
  3 NULLS FIRST,
  4 NULLS FIRST
