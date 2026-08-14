WITH _q_0 AS (
  SELECT
    c_comment AS comment
  FROM tpch.customer
  ORDER BY
    c_custkey NULLS FIRST
  LIMIT 3
)
SELECT
  _s0.value AS char,
  COUNT(*) AS n
FROM _q_0 AS _q_0
JOIN LATERAL FLATTEN(REGEXP_EXTRACT_ALL(_q_0.comment, '.{1}')) AS _s0(seq, key, path, index, value, this)
  ON _s0.value <> ''
GROUP BY
  1
