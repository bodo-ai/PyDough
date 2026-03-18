SELECT
  CAST((
    100.0 * COUNT_IF(LOWER(school_metro) = 'suburban')
  ) AS DOUBLE) / COUNT(*) AS percentage_suburban
FROM main.projects
WHERE
  LOWER(school_city) = 'santa barbara'
