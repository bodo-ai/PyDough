SELECT
  (
    100.0 * SUM(LOWER(school_metro) = 'suburban')
  ) / COUNT(*) AS percentage_suburban
FROM MAIN.PROJECTS
WHERE
  LOWER(school_city) = 'santa barbara'
