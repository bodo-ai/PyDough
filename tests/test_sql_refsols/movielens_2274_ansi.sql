SELECT
  CASE
    WHEN COUNT(*) > 0
    THEN (
      100.0 * SUM(LOWER(u_gender) = 'f')
    ) / COUNT(*)
    ELSE 0.0
  END AS percentage_of_female_users
FROM main.users
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.u2base
    WHERE
      rating = 2 AND userid = users.userid
  )
