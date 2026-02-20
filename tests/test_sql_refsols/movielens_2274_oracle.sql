SELECT
  CASE
    WHEN COUNT(*) > 0
    THEN (
      100.0 * SUM(LOWER(USERS.u_gender) = 'f')
    ) / COUNT(*)
    ELSE 0.0
  END AS percentage_of_female_users
FROM MAIN.USERS USERS
JOIN MAIN.U2BASE U2BASE
  ON U2BASE.rating = 2 AND U2BASE.userid = USERS.userid
