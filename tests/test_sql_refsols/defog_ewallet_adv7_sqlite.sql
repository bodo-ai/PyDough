WITH _table_alias_0 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
), _table_alias_1 AS (
  SELECT
    user_setting_snapshot.created_at AS created_at,
    user_setting_snapshot.marketing_opt_in AS marketing_opt_in,
    user_setting_snapshot.user_id AS user_id
  FROM main.user_setting_snapshot AS user_setting_snapshot
), _t AS (
  SELECT
    _table_alias_1.created_at AS created_at_1,
    _table_alias_1.marketing_opt_in AS marketing_opt_in,
    _table_alias_0.uid AS uid,
    ROW_NUMBER() OVER (PARTITION BY _table_alias_0.uid ORDER BY _table_alias_1.created_at DESC) AS _w
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.uid = _table_alias_1.user_id
)
SELECT
  _t.uid AS uid,
  _t.marketing_opt_in AS marketing_opt_in
FROM _t AS _t
WHERE
  _t._w = 1
