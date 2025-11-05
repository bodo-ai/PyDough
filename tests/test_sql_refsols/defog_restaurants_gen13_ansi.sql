SELECT
  COALESCE(SUM(rating > 4.0), 0) / CASE
    WHEN (
      NOT SUM(rating < 4.0) IS NULL AND SUM(rating < 4.0) <> 0
    )
    THEN COALESCE(SUM(rating < 4.0), 0)
    ELSE NULL
  END AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'new york'
