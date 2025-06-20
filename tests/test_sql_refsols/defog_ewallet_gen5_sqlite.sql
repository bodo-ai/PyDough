SELECT
  _s0.username AS username,
  _s0.email AS email,
  _s0.created_at AS created_at
FROM main.users AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.notifications AS _s1
    JOIN main.users AS _s2
      ON _s1.user_id = _s2.uid
    WHERE
      _s0.uid = _s1.user_id
      AND _s1.created_at <= DATETIME(_s2.created_at, '1 year')
      AND _s1.created_at >= _s2.created_at
  )
