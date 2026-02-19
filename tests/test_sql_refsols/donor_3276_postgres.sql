SELECT
  (
    100.0 * SUM(CASE WHEN LOWER(school_metro) = 'suburban' THEN 1 ELSE 0 END)
  ) / COUNT(*) AS percentage_suburban
FROM main.projects
WHERE
  LOWER(school_city) = 'santa barbara'
