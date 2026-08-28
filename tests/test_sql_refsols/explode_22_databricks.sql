SELECT
  region.r_name AS name,
  _s0.val AS letter
FROM tpch.region AS region, LATERAL POSEXPLODE(ARRAY('A', 'E', 'I')) AS _s0(idx, val)
WHERE
  CONTAINS(region.r_name, _s0.val)
