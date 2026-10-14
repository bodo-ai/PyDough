WITH _s1 AS (
  SELECT
    userid
  FROM main.u2base
  WHERE
    rating = 2
)
SELECT
  CASE
    WHEN COUNT(*) > 0
    THEN (
      100.0 * COUNT_IF(LOWER(users.u_gender) = 'f')
    ) / COUNT(*)
    ELSE 0.0
  END AS percentage_of_female_users
FROM main.users AS users
SEMI JOIN _s1 AS _s1
  ON _s1.userid = users.userid
