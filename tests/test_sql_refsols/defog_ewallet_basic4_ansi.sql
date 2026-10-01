WITH _s1 AS (
  SELECT
    user_id
  FROM main.notifications
  WHERE
    type = 'transaction'
)
SELECT
  users.uid AS user_id
FROM main.users AS users
SEMI JOIN _s1 AS _s1
  ON _s1.user_id = users.uid
