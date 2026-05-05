SELECT
  okey,
  total,
  ROW_NUMBER() OVER (ORDER BY total DESC) AS rank
FROM order_summary_t9
ORDER BY
  2 DESC
LIMIT 5
