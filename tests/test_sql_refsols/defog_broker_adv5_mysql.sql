WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbDpClose,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    SUM(sbdpclose) AS sum_sbDpClose,
    CONCAT_WS('-', YEAR(sbdpdate), LPAD(MONTH(sbdpdate), 2, '0')) AS month,
    sbdptickerid AS sbDpTickerId
  FROM main.sbDailyPrice
  GROUP BY
    CONCAT_WS('-', YEAR(sbdpdate), LPAD(MONTH(sbdpdate), 2, '0')),
    sbdptickerid
), _t0 AS (
  SELECT
    SUM(_s0.sum_sbDpClose) / SUM(_s0.count_sbDpClose) AS avg_close,
    MAX(_s0.max_high) AS max_high,
    MIN(_s0.min_low) AS min_low,
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
  avg_close,
  max_high,
  min_low,
  (
    avg_close - LAG(avg_close, 1) OVER (PARTITION BY sbTickerSymbol ORDER BY CASE WHEN month IS NULL THEN 1 ELSE 0 END, month)
  ) / LAG(avg_close, 1) OVER (PARTITION BY sbTickerSymbol ORDER BY CASE WHEN month IS NULL THEN 1 ELSE 0 END, month) AS momc
FROM _t0
