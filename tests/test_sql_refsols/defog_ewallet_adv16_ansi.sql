WITH _s3 AS (
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
  _s0.username,
  _s3.total_unread_notifs
FROM main.users AS _s0
JOIN _s3 AS _s3
  ON _s0.uid = _s3.user_id
WHERE
  LOWER(_s0.country) = 'us'
