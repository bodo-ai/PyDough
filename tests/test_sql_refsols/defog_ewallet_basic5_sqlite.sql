SELECT
  uid,
  username
FROM (
  SELECT
    uid,
    username
  FROM main.users
)
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        user_id
      FROM main.notifications
    )
    WHERE
      uid = user_id
  )
