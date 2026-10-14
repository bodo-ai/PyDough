SELECT
  sbDpTickerId COLLATE utf8mb4_bin AS ticker_id,
  sbDpHigh - 185 AS exp,
  CASE WHEN sbDpHigh = 185 THEN 0 ELSE CASE WHEN sbDpHigh < 185 THEN -1 ELSE 1 END END AS sign_exp,
  CASE
    WHEN -1 * (
      sbDpHigh - 185
    ) = 0
    THEN 0
    ELSE CASE WHEN -1 * (
      sbDpHigh - 185
    ) < 0 THEN -1 ELSE 1 END
  END AS sign_neg_exp_a,
  CASE
    WHEN -1.0 * (
      sbDpHigh - 185
    ) = 0
    THEN 0
    ELSE CASE WHEN -1.0 * (
      sbDpHigh - 185
    ) < 0 THEN -1 ELSE 1 END
  END AS sign_neg_exp_b,
  1 AS sign_pos,
  -1 AS sign_neg,
  0 AS sign_zero,
  0 AS sign_exp_zero,
  CASE
    WHEN ABS(sbDpHigh - 185) = 0
    THEN 0
    ELSE CASE WHEN ABS(sbDpHigh - 185) < 0 THEN -1 ELSE 1 END
  END AS sign_abs_exp,
  CASE
    WHEN -1 * ABS(sbDpHigh - 185) = 0
    THEN 0
    ELSE CASE WHEN -1 * ABS(sbDpHigh - 185) < 0 THEN -1 ELSE 1 END
  END AS sign_neg_abs_exp
FROM main.sbDailyPrice
ORDER BY
  sbDpDate,
  1
LIMIT 5
