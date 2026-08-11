WITH _t0 AS (
  SELECT
    marketing_opt_in,
    user_id
  FROM main.user_setting_snapshot
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC NULLS FIRST) = 1
)
SELECT
  users.uid,
  _t0.marketing_opt_in
FROM main.users AS users
JOIN _t0 AS _t0
  ON _t0.user_id = users.uid
