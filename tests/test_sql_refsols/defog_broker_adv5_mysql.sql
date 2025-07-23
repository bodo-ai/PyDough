WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbDpClose,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    CONCAT_WS('-', YEAR(sbdpdate), LPAD(MONTH(sbdpdate), 2, '0')) AS month,
    SUM(sbdpclose) AS sum_sbDpClose,
    sbdptickerid AS sbDpTickerId
  FROM main.sbDailyPrice
  GROUP BY
    CONCAT_WS('-', YEAR(sbdpdate), LPAD(MONTH(sbdpdate), 2, '0')),
    sbdptickerid
), _t0 AS (
  SELECT
    MAX(_s0.max_high) AS max_high,
    MIN(_s0.min_low) AS min_low,
    SUM(_s0.count_sbDpClose) AS sum_count_sbDpClose,
    SUM(_s0.sum_sbDpClose) AS sum_sum_sbDpClose,
    _s0.month,
    sbTicker.sbtickersymbol AS sbTickerSymbol
  FROM _s0 AS _s0
  JOIN main.sbTicker AS sbTicker
    ON _s0.sbDpTickerId = sbTicker.sbtickerid
  GROUP BY
    _s0.month,
    sbTicker.sbtickersymbol
)
SELECT
  sbTickerSymbol AS symbol,
  month,
  sum_sum_sbDpClose / sum_count_sbDpClose AS avg_close,
  max_high,
  min_low,
  (
    (
      sum_sum_sbDpClose / sum_count_sbDpClose
    ) - LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY sbTickerSymbol ORDER BY CASE WHEN month IS NULL THEN 1 ELSE 0 END, month)
  ) / LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY sbTickerSymbol ORDER BY CASE WHEN month IS NULL THEN 1 ELSE 0 END, month) AS momc
FROM _t0
