WITH _table_alias_0 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
), _table_alias_1 AS (
  SELECT
    wallet_user_balance_daily.user_id AS user_id
  FROM main.wallet_user_balance_daily AS wallet_user_balance_daily
), _u_0 AS (
  SELECT
    _table_alias_1.user_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.user_id
), _table_alias_6 AS (
  SELECT
    _table_alias_0.uid AS uid
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _u_0 AS _u_0
    ON _table_alias_0.uid = _u_0._u_1
  WHERE
    NOT _u_0._u_1 IS NULL
), _u_2 AS (
  SELECT
    _table_alias_3.user_id AS _u_3
  FROM _table_alias_1 AS _table_alias_3
  GROUP BY
    _table_alias_3.user_id
), _table_alias_4 AS (
  SELECT
    _table_alias_2.uid AS uid
  FROM _table_alias_0 AS _table_alias_2
  LEFT JOIN _u_2 AS _u_2
    ON _table_alias_2.uid = _u_2._u_3
  WHERE
    NOT _u_2._u_3 IS NULL
), _table_alias_5 AS (
  SELECT
    wallet_user_balance_daily.balance AS balance,
    wallet_user_balance_daily.updated_at AS updated_at,
    wallet_user_balance_daily.user_id AS user_id
  FROM main.wallet_user_balance_daily AS wallet_user_balance_daily
), _t1 AS (
  SELECT
    _table_alias_5.balance AS balance,
    _table_alias_4.uid AS uid,
    _table_alias_5.updated_at AS updated_at
  FROM _table_alias_4 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.uid = _table_alias_5.user_id
), _t AS (
  SELECT
    _t1.balance AS balance,
    _t1.uid AS uid,
    _t1.updated_at AS updated_at,
    ROW_NUMBER() OVER (PARTITION BY _t1.uid ORDER BY _t1.updated_at DESC) AS _w
  FROM _t1 AS _t1
), _t0 AS (
  SELECT
    _t.balance AS balance,
    _t.uid AS uid,
    _t.updated_at AS updated_at,
    _t._w AS _w
  FROM _t AS _t
  WHERE
    _t._w = 1
), _table_alias_7 AS (
  SELECT
    _t0.balance AS balance,
    _t0.uid AS uid
  FROM _t0 AS _t0
)
SELECT
  _table_alias_6.uid AS user_id,
  _table_alias_7.balance AS latest_balance
FROM _table_alias_6 AS _table_alias_6
LEFT JOIN _table_alias_7 AS _table_alias_7
  ON _table_alias_6.uid = _table_alias_7.uid
