SELECT
  country,
  ROUND((
    100 * COUNT_IF(discounted)
  ) / COUNT(*), 2) AS pct_discounted
FROM sale
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
