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
FROM _q_0 AS _q_0, UNNEST(SPLIT(_q_0.comment, '')) WITH ORDINALITY AS _s0(val, idx)
WHERE
  _s0.val <> ''
GROUP BY
  1
