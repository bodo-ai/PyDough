SELECT
  pkey,
  pname,
  psize
FROM parts_summary_t11
ORDER BY
  3 DESC,
  1
LIMIT 5
