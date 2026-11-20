SELECT
  region.r_name AS name,
  _s0.val AS letter
FROM tpch_s3_pq.tpch_sf1.region AS region, LATERAL POSEXPLODE(ARRAY('A', 'E', 'I')) AS _s0(idx, val)
WHERE
  CONTAINS(region.r_name, _s0.val)
