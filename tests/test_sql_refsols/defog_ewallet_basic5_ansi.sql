SELECT
  uid,
  username
FROM main.users
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.notifications
    WHERE
      user_id = users.uid
  )
