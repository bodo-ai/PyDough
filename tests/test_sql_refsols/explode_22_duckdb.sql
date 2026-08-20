SELECT
  region.r_name AS name,
  _s0.val AS letter
FROM tpch.region AS region, LATERAL (
  SELECT
    UNNEST(['A', 'E', 'I']) AS _col_0,
    GENERATE_SUBSCRIPTS(['A', 'E', 'I'], 1) - 1 AS _col_1
) AS _s0(val, idx)
WHERE
  region.r_name LIKE CONCAT('%', _s0.val, '%')
