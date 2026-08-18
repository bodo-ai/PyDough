WITH _s1 AS (
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
FROM _s1 AS _s1, LATERAL FLATTEN(REGEXP_EXTRACT_ALL(_s1.comment, '.{1}')) AS _s0(seq, key, path, index, value, this)
WHERE
  _s0.value <> ''
GROUP BY
  1
