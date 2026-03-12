SELECT
  uid AS user_id
FROM ewallet.users
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM ewallet.notifications
    WHERE
      type = 'transaction' AND user_id = users.uid
  )
