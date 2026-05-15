WITH _u_0 AS (
  SELECT
    userid AS _u_1
  FROM main.u2base
  WHERE
    rating = 2
  GROUP BY
    1
)
SELECT
  CASE
    WHEN COUNT(*) > 0
    THEN (
      100.0 * SUM(LOWER(users.u_gender) = 'f')
    ) / COUNT(*)
    ELSE 0.0
  END AS percentage_of_female_users
FROM main.users AS users
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = users.userid
WHERE
  NOT _u_0._u_1 IS NULL
