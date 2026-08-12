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
  _s0.idx
FROM _q_0 AS _q_0
CROSS JOIN LATERAL (
  SELECT
    UNNEST(STR_SPLIT(_q_0.name, '#')) AS _col_0,
    GENERATE_SUBSCRIPTS(STR_SPLIT(_q_0.name, '#'), 1) - 1 AS _col_1
) AS _s0(val, idx)
ORDER BY
  _q_0.name NULLS FIRST,
  2 NULLS FIRST
