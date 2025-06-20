SELECT
  _s0.username AS username,
  _s0.email AS email,
  _s0.created_at AS created_at
FROM main.users AS _s0
JOIN main.notifications AS _s1
  ON _s0.uid = _s1.user_id
JOIN main.users AS _s2
  ON _s1.user_id = _s2.uid
WHERE
  _s1.created_at <= DATE_ADD(CAST(_s2.created_at AS TIMESTAMP), 1, 'YEAR')
  AND _s1.created_at >= _s2.created_at
