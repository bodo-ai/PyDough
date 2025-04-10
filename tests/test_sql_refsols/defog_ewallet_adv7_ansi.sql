WITH _t0_2 AS (
  SELECT
    user_setting_snapshot.marketing_opt_in,
    users.uid
  FROM main.users AS users
  JOIN main.user_setting_snapshot AS user_setting_snapshot
    ON user_setting_snapshot.user_id = users.uid
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY users.uid ORDER BY user_setting_snapshot.created_at DESC NULLS FIRST) = 1
)
SELECT
  uid,
  marketing_opt_in
FROM _t0_2
