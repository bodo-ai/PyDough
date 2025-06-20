SELECT
  _s0.mid AS merchant
FROM main.merchants AS _s0
JOIN main.wallet_transactions_daily AS _s1
  ON _s0.mid = _s1.receiver_id AND _s1.receiver_type = 1
