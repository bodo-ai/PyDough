SELECT
  uid AS user_id
FROM main.users
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.notifications
    WHERE
      type = 'transaction' AND user_id = users.uid
  )
