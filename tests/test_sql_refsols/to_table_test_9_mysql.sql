SELECT
  okey,
  total,
  ROW_NUMBER() OVER (ORDER BY CASE WHEN total IS NULL THEN 1 ELSE 0 END DESC, total DESC) AS `rank`
FROM order_summary_t9
ORDER BY
  2 DESC
LIMIT 5
