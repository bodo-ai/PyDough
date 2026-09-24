SELECT
  SBDPTICKERID AS ticker_id,
  SBDPHIGH - 185 AS exp,
  CASE WHEN SBDPHIGH = 185 THEN 0 ELSE CASE WHEN SBDPHIGH < 185 THEN -1 ELSE 1 END END AS sign_exp,
  CASE
    WHEN -1 * (
      SBDPHIGH - 185
    ) = 0
    THEN 0
    ELSE CASE WHEN -1 * (
      SBDPHIGH - 185
    ) < 0 THEN -1 ELSE 1 END
  END AS sign_neg_exp_a,
  CASE
    WHEN -1.0 * (
      SBDPHIGH - 185
    ) = 0
    THEN 0
    ELSE CASE WHEN -1.0 * (
      SBDPHIGH - 185
    ) < 0 THEN -1 ELSE 1 END
  END AS sign_neg_exp_b,
  1 AS sign_pos,
  -1 AS sign_neg,
  0 AS sign_zero,
  0 AS sign_exp_zero,
  CASE
    WHEN ABS(SBDPHIGH - 185) = 0
    THEN 0
    ELSE CASE WHEN ABS(SBDPHIGH - 185) < 0 THEN -1 ELSE 1 END
  END AS sign_abs_exp,
  CASE
    WHEN -1 * ABS(SBDPHIGH - 185) = 0
    THEN 0
    ELSE CASE WHEN -1 * ABS(SBDPHIGH - 185) < 0 THEN -1 ELSE 1 END
  END AS sign_neg_abs_exp
FROM MAIN.SBDAILYPRICE
ORDER BY
  SBDPDATE NULLS FIRST,
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
