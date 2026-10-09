WITH _s3 AS (
  SELECT
    notifications.user_id
  FROM main.notifications AS notifications
  JOIN main.users AS users
    ON notifications.created_at <= DATE_ADD(CAST(users.created_at AS TIMESTAMP), 1, 'YEAR')
    AND notifications.created_at >= users.created_at
    AND notifications.user_id = users.uid
)
SELECT
  users.username,
  users.email,
  users.created_at
FROM main.users AS users
ANTI JOIN _s3 AS _s3
  ON _s3.user_id = users.uid
