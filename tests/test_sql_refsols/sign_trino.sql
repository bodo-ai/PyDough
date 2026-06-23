SELECT
  sbdptickerid AS ticker_id,
  sbdphigh - 185 AS exp,
  SIGN(sbdphigh - 185) AS sign_exp,
  SIGN(-1 * (
    sbdphigh - 185
  )) AS sign_neg_exp_a,
  SIGN(-1.0 * (
    sbdphigh - 185
  )) AS sign_neg_exp_b,
  SIGN(13) AS sign_pos,
  SIGN(-0.5) AS sign_neg,
  SIGN(0) AS sign_zero,
  SIGN(0) AS sign_exp_zero,
  SIGN(ABS(sbdphigh - 185)) AS sign_abs_exp,
  SIGN(-1 * ABS(sbdphigh - 185)) AS sign_neg_abs_exp
FROM main.sbdailyprice
ORDER BY
  sbdpdate NULLS FIRST,
  1 NULLS FIRST
LIMIT 5
