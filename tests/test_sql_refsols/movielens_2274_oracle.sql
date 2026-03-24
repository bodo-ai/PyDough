WITH "_u_0" AS (
  SELECT
    userid AS "_u_1"
  FROM MAIN.U2BASE
  WHERE
    rating = 2
  GROUP BY
    userid
)
SELECT
  CASE
    WHEN COUNT(*) > 0
    THEN (
      100.0 * SUM(LOWER(USERS.u_gender) = 'f')
    ) / COUNT(*)
    ELSE 0.0
  END AS percentage_of_female_users
FROM MAIN.USERS USERS
LEFT JOIN "_u_0" "_u_0"
  ON USERS.userid = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
