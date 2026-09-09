SELECT
  region.r_name AS name,
  _s0.val AS letter
FROM tpch.region AS region, UNNEST(ARRAY['A', 'E', 'I']) WITH ORDINALITY AS _s0(val, idx)
WHERE
  region.r_name LIKE CONCAT('%', _s0.val, '%')
