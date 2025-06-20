SELECT
  _s0.uid AS user_id
FROM main.users AS _s0
JOIN main.notifications AS _s1
  ON _s0.uid = _s1.user_id AND _s1.type = 'transaction'
