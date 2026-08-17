WITH _q_0 AS (
  SELECT
    c_comment AS comment
  FROM tpch.customer
  ORDER BY
    c_custkey NULLS FIRST
  LIMIT 3
)
SELECT
  _s0.val AS char,
  COUNT(*) AS n
FROM _q_0 AS _q_0, LATERAL (
  SELECT
    UNNEST(REGEXP_SPLIT_TO_ARRAY(_q_0.comment, '')) AS _col_0,
    GENERATE_SUBSCRIPTS(REGEXP_SPLIT_TO_ARRAY(_q_0.comment, ''), 1) - 1 AS _col_1
) AS _s0(val, idx)
WHERE
  _s0.val <> ''
GROUP BY
  1
