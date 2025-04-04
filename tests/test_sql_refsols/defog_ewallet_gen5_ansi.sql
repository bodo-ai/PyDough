WITH _t0 AS (
  SELECT
    created_at AS created_at,
    user_id AS user_id
  FROM main.notifications
), _t1 AS (
  SELECT
    created_at AS created_at,
    uid AS uid
  FROM main.users
), _t0_2 AS (
  SELECT
    _t0.created_at AS created_at,
    _t1.created_at AS created_at_1,
    _t0.user_id AS user_id
  FROM _t0 AS _t0
  LEFT JOIN _t1 AS _t1
    ON _t0.user_id = _t1.uid
  WHERE
    _t0.created_at <= DATE_ADD(CAST(_t1.created_at AS TIMESTAMP), 1, 'YEAR')
    AND _t0.created_at >= _t1.created_at
), _t3 AS (
  SELECT
    _t0.user_id AS user_id
  FROM _t0_2 AS _t0
), _t2 AS (
  SELECT
    created_at AS created_at,
    email AS email,
    uid AS uid,
    username AS username
  FROM main.users
)
SELECT
  username AS username,
  email AS email,
  created_at AS created_at
FROM _t2
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t3
    WHERE
      uid = user_id
  )
