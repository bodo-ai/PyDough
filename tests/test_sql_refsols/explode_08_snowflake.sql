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
  _s0.index - 1 AS idx1,
  _s2.index - 1 AS idx2,
  _s4.index - 1 AS idx3,
  _s4.value AS val3
FROM _s1 AS _s1
CROSS JOIN LATERAL SPLIT_TO_TABLE(_s1.comment, '.') AS _s0
CROSS JOIN LATERAL SPLIT_TO_TABLE(_s0.value, ' ') AS _s2, LATERAL SPLIT_TO_TABLE(_s2.value, ',') AS _s4
WHERE
  _s4.value <> ''
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST,
  3 NULLS FIRST,
  4 NULLS FIRST
