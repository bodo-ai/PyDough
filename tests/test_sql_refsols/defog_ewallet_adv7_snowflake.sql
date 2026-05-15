WITH _t0 AS (
  SELECT
    marketing_opt_in,
    user_id
  FROM ewallet.user_setting_snapshot
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) = 1
)
SELECT
  users.uid,
  _t0.marketing_opt_in
FROM ewallet.users AS users
JOIN _t0 AS _t0
  ON _t0.user_id = users.uid
