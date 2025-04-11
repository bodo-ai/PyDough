WITH _t1_2 AS (
  SELECT
    COUNT() AS agg_0,
    user_id
  FROM main.notifications
  WHERE
    status = 'unread' AND type = 'promotion'
  GROUP BY
    user_id
)
SELECT
  users.username,
  COALESCE(_t1.agg_0, 0) AS total_unread_notifs
FROM main.users AS users
JOIN _t1_2 AS _t1
  ON _t1.user_id = users.uid
WHERE
  LOWER(users.country) = 'us'
