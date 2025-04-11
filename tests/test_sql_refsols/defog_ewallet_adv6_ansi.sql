WITH _s0 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
), _s1 AS (
  SELECT
    wallet_user_balance_daily.user_id AS user_id
  FROM main.wallet_user_balance_daily AS wallet_user_balance_daily
), _s6 AS (
  SELECT
    _s0.uid AS uid
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s0.uid = _s1.user_id
    )
), _s4 AS (
  SELECT
    _s2.uid AS uid
  FROM _s0 AS _s2
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s3
      WHERE
        _s2.uid = _s3.user_id
    )
), _s5 AS (
  SELECT
    wallet_user_balance_daily.balance AS balance,
    wallet_user_balance_daily.updated_at AS updated_at,
    wallet_user_balance_daily.user_id AS user_id
  FROM main.wallet_user_balance_daily AS wallet_user_balance_daily
), _t1 AS (
  SELECT
    _s5.balance AS balance,
    _s4.uid AS uid,
    _s5.updated_at AS updated_at
  FROM _s4 AS _s4
  JOIN _s5 AS _s5
    ON _s4.uid = _s5.user_id
), _t0 AS (
  SELECT
    _t1.balance AS balance,
    _t1.uid AS uid
  FROM _t1 AS _t1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _t1.uid ORDER BY _t1.updated_at DESC NULLS FIRST) = 1
), _s7 AS (
  SELECT
    _t0.balance AS balance,
    _t0.uid AS uid
  FROM _t0 AS _t0
)
SELECT
  _s6.uid AS user_id,
  _s7.balance AS latest_balance
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.uid = _s7.uid
