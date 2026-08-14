WITH _q_0 AS (
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
FROM _q_0 AS _q_0
CROSS JOIN UNNEST(SPLIT(_q_0.name, '#')) WITH ORDINALITY AS _s0(val, idx)
ORDER BY
  _q_0.name NULLS FIRST,
  2 NULLS FIRST
