WITH _s1 AS (
  SELECT
    c_name AS name
  FROM tpch.customer
  ORDER BY
    c_custkey NULLS FIRST
  LIMIT 5
)
SELECT
  _s0.val,
  _s0.idx - 1 AS idx
FROM _s1 AS _s1
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(_s1.name, '#')) WITH ORDINALITY AS _s0(val, idx)
ORDER BY
  _s1.name NULLS FIRST,
  2 NULLS FIRST
