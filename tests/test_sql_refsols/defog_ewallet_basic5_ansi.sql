WITH _s1 AS (
  SELECT
    user_id
  FROM main.notifications
)
SELECT
  users.uid,
  users.username
FROM main.users AS users
ANTI JOIN _s1 AS _s1
  ON _s1.user_id = users.uid
