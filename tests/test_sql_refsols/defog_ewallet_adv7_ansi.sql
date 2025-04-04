WITH _t0_2 AS (
  SELECT
    user_setting_snapshot.marketing_opt_in AS marketing_opt_in,
    users.uid AS uid
  FROM main.users AS users
  JOIN main.user_setting_snapshot AS user_setting_snapshot
    ON user_setting_snapshot.user_id = users.uid
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY users.uid ORDER BY user_setting_snapshot.created_at DESC NULLS FIRST) = 1
)
SELECT
  _t0.uid AS uid,
  _t0.marketing_opt_in AS marketing_opt_in
FROM _t0_2 AS _t0
