SELECT
  uid AS user_id
FROM (
  SELECT
    uid
  FROM main.users
)
SEMI JOIN (
  SELECT
    user_id
  FROM (
    SELECT
      type AS notification_type,
      user_id
    FROM main.notifications
  )
  WHERE
    notification_type = 'transaction'
)
  ON uid = user_id
