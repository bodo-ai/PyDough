WITH _q_0 AS (
  SELECT
    c_name AS name
  FROM tpch.customer
  ORDER BY
    c_custkey NULLS FIRST
  LIMIT 5
)
SELECT
  _s0.value AS val,
  _s0.index - 1 AS idx
FROM _q_0 AS _q_0
CROSS JOIN LATERAL SPLIT_TO_TABLE(_q_0.name, '#') AS _s0
ORDER BY
  _q_0.name NULLS FIRST,
  2 NULLS FIRST
