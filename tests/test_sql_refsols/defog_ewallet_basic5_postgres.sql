WITH _u_0 AS (
  SELECT
    user_id AS _u_1
  FROM main.notifications
  GROUP BY
    user_id
)
SELECT
  users.uid,
  users.username
FROM main.users AS users
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = users.uid
WHERE
  _u_0._u_1 IS NULL
