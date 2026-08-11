WITH _q_0 AS (
  SELECT
    c_name AS name
  FROM tpch.customer
  ORDER BY
    c_custkey
  LIMIT 5
)
SELECT
  _s0.val,
  _s0.idx
FROM _q_0 AS _q_0
CROSS JOIN LATERAL POSEXPLODE(SPLIT(_q_0.name, '\\Q#\\E')) AS _s0(idx, val)
ORDER BY
  _q_0.name,
  2
