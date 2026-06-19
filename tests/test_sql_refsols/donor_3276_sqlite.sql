SELECT
  CAST((
    100.0 * SUM(IIF(LOWER(school_metro) = 'suburban', 1, 0))
  ) AS REAL) / COUNT(*) AS percentage_suburban
FROM main.projects
WHERE
  LOWER(school_city) = 'santa barbara'
