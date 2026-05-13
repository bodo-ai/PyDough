SELECT
  pkey,
  pname,
  psize
FROM memory.default.parts_summary_t11
ORDER BY
  3 DESC,
  1 NULLS FIRST
LIMIT 5
