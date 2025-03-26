SELECT
  uid AS user_id
FROM (
  SELECT
    uid
  FROM main.users
)
WHERE
  EXISTS(
    SELECT
      1
    FROM (
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
    WHERE
      uid = user_id
  )
