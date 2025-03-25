SELECT
  uid,
  marketing_opt_in
FROM (
  SELECT
    *
  FROM (
    SELECT
      created_at AS created_at_1,
      marketing_opt_in,
      uid
    FROM (
      SELECT
        uid
      FROM main.users
    ) AS _table_alias_0
    INNER JOIN (
      SELECT
        created_at,
        marketing_opt_in,
        user_id
      FROM main.user_setting_snapshot
    ) AS _table_alias_1
      ON uid = user_id
  ) AS _t1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY created_at_1 DESC NULLS FIRST) = 1
) AS _t0
