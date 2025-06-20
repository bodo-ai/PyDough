SELECT
  _s0.uid AS user_id
FROM main.users AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.notifications AS _s1
    WHERE
      _s0.uid = _s1.user_id AND _s1.type = 'transaction'
  )
