SELECT
  _s0.uid AS uid,
  _s0.username AS username
FROM main.users AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.notifications AS _s1
    WHERE
      _s0.uid = _s1.user_id
  )
