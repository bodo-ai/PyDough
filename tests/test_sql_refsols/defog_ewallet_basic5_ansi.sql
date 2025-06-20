SELECT
  _s0.uid AS uid,
  _s0.username AS username
FROM main.users AS _s0
JOIN main.notifications AS _s1
  ON _s0.uid = _s1.user_id
