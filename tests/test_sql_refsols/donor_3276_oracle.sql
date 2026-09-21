SELECT
  (
    100.0 * SUM(LOWER(SCHOOL_METRO) = 'suburban')
  ) / COUNT(*) AS percentage_suburban
FROM MAIN.PROJECTS
WHERE
  LOWER(SCHOOL_CITY) = 'santa barbara'
