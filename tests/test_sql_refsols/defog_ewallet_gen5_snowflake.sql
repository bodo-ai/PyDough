WITH _u_0 AS (
  SELECT
    notifications.user_id AS _u_1
  FROM ewallet.notifications AS notifications
  JOIN ewallet.users AS users
    ON notifications.created_at <= DATEADD(YEAR, 1, CAST(users.created_at AS TIMESTAMP))
    AND notifications.created_at >= users.created_at
    AND notifications.user_id = users.uid
  GROUP BY
    1
)
SELECT
  users.username,
  users.email,
  users.created_at
FROM ewallet.users AS users
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = users.uid
WHERE
  _u_0._u_1 IS NULL
