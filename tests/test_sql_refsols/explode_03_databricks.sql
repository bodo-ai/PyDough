WITH _s1 AS (
  SELECT
    c_name AS name
  FROM tpch_s3_pq.tpch_sf1.customer
  ORDER BY
    c_custkey
  LIMIT 5
)
SELECT
  _s0.val,
  _s0.idx
FROM _s1 AS _s1
CROSS JOIN LATERAL POSEXPLODE(SPLIT(_s1.name, '\\Q#\\E')) AS _s0(idx, val)
ORDER BY
  _s1.name,
  2
