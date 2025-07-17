WITH _s1 AS (
  SELECT
    COUNT(*) AS total_unread_notifs,
    user_id
  FROM main.notifications
  WHERE
    status = 'unread' AND type = 'promotion'
  GROUP BY
    user_id
)
SELECT
  users.username,
  _s1.total_unread_notifs
FROM main.users AS users
JOIN _s1 AS _s1
  ON _s1.user_id = users.uid
WHERE
  LOWER(users.country) = 'us'
