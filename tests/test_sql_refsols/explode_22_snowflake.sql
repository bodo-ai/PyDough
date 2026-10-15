SELECT
  region.r_name AS name,
  _s0.value AS letter
FROM tpch.region AS region, LATERAL FLATTEN(['A', 'E', 'I']) AS _s0(seq, key, path, index, value, this)
WHERE
  CONTAINS(region.r_name, _s0.value)
