SELECT
  uid,
  username
FROM (
  SELECT
    uid,
    username
  FROM main.users
)
ANTI JOIN (
  SELECT
    user_id
  FROM main.notifications
)
  ON uid = user_id
