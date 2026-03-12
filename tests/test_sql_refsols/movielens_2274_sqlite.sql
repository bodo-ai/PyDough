SELECT
  IIF(COUNT(*) > 0, CAST((
    100.0 * SUM(LOWER(u_gender) = 'f')
  ) AS REAL) / COUNT(*), 0.0) AS percentage_of_female_users
FROM main.users
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.u2base
    WHERE
      rating = 2 AND userid = users.userid
  )
