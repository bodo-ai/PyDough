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
  IFF(COUNT(*) > 0, (
    100.0 * COUNT_IF(LOWER(users.u_gender) = 'f')
  ) / COUNT(*), 0.0) AS percentage_of_female_users
FROM main.users AS users
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = users.userid
WHERE
  NOT _u_0._u_1 IS NULL
