SELECT
  uid,
  username
FROM ewallet.users
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM ewallet.notifications
    WHERE
      user_id = users.uid
  )
