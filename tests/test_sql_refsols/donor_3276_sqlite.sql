SELECT
  CAST((
    100.0 * SUM(LOWER(school_metro) = 'suburban')
  ) AS REAL) / COUNT(*) AS percentage_suburban
FROM main.projects
WHERE
  LOWER(school_city) = 'santa barbara'
