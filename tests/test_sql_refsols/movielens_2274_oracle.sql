WITH "_u_0" AS (
  SELECT
    USERID AS "_u_1"
  FROM MAIN.U2BASE
  WHERE
    RATING = 2
  GROUP BY
    USERID
)
SELECT
  CASE
    WHEN COUNT(*) > 0
    THEN (
      100.0 * SUM(LOWER(USERS.U_GENDER) = 'f')
    ) / COUNT(*)
    ELSE 0.0
  END AS percentage_of_female_users
FROM MAIN.USERS USERS
LEFT JOIN "_u_0" "_u_0"
  ON USERS.USERID = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
