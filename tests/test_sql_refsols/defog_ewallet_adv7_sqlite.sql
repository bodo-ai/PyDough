WITH _t AS (
  SELECT
    user_setting_snapshot.marketing_opt_in,
    users.uid,
    ROW_NUMBER() OVER (PARTITION BY users.uid ORDER BY user_setting_snapshot.created_at DESC) AS _w
  FROM main.users AS users
  JOIN main.user_setting_snapshot AS user_setting_snapshot
    ON user_setting_snapshot.user_id = users.uid
)
SELECT
  uid,
  marketing_opt_in
FROM _t
WHERE
  _w = 1
