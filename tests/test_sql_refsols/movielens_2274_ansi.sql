SELECT
  CASE
    WHEN COUNT(*) > 0
    THEN (
      100.0 * SUM(LOWER(users.u_gender) = 'f')
    ) / COUNT(*)
    ELSE 0.0
  END AS percentage_of_female_users
FROM main.users AS users
JOIN main.u2base AS u2base
  ON u2base.rating = 2 AND u2base.userid = users.userid
