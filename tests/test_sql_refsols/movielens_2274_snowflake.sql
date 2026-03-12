SELECT
  IFF(COUNT(*) > 0, (
    100.0 * COUNT_IF(LOWER(u_gender) = 'f')
  ) / COUNT(*), 0.0) AS percentage_of_female_users
FROM main.users
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.u2base
    WHERE
      rating = 2 AND userid = users.userid
  )
