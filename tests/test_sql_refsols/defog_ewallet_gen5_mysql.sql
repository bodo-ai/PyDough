WITH _u_0 AS (
  SELECT
    notifications.user_id AS _u_1
  FROM main.notifications AS notifications
  JOIN main.users AS users
    ON notifications.created_at <= DATE_ADD(CAST(users.created_at AS DATETIME), INTERVAL '1' YEAR)
    AND notifications.created_at >= users.created_at
    AND notifications.user_id = users.uid
  GROUP BY
    notifications.user_id
)
SELECT
  users.username,
  users.email,
  users.created_at
FROM main.users AS users
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = users.uid
WHERE
  _u_0._u_1 IS NULL
