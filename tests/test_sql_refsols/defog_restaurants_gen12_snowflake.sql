SELECT
  COALESCE(COUNT_IF(rating > 4.0), 0) / CASE
    WHEN (
      COUNT_IF(rating < 4.0) <> 0 AND NOT COUNT_IF(rating < 4.0) IS NULL
    )
    THEN COALESCE(COUNT_IF(rating < 4.0), 0)
    ELSE NULL
  END AS ratio
FROM main.restaurant
