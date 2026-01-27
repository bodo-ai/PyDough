WITH _s0 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdpdate AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(sbdpdate AS DATETIME)), 2, '0')
    ) AS month,
    sbdptickerid AS sbDpTickerId,
    COUNT(sbdpclose) AS count_sbDpClose,
    MAX(sbdphigh) AS max_sbDpHigh,
    MIN(sbdplow) AS min_sbDpLow,
    SUM(sbdpclose) AS sum_sbDpClose
  FROM broker.sbDailyPrice
  GROUP BY
    1,
    2
), _t0 AS (
  SELECT
    _s0.month,
    sbTicker.sbtickersymbol AS sbTickerSymbol,
    MAX(_s0.max_sbDpHigh) AS max_max_sbDpHigh,
    MIN(_s0.min_sbDpLow) AS min_min_sbDpLow,
    SUM(_s0.count_sbDpClose) AS sum_count_sbDpClose,
    SUM(_s0.sum_sbDpClose) AS sum_sum_sbDpClose
  FROM _s0 AS _s0
  JOIN broker.sbTicker AS sbTicker
    ON _s0.sbDpTickerId = sbTicker.sbtickerid
  GROUP BY
    1,
    2
)
SELECT
  sbTickerSymbol AS symbol,
  month,
  sum_sum_sbDpClose / sum_count_sbDpClose AS avg_close,
  max_max_sbDpHigh AS max_high,
  min_min_sbDpLow AS min_low,
  (
    (
      sum_sum_sbDpClose / sum_count_sbDpClose
    ) - LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY sbTickerSymbol ORDER BY CASE WHEN month COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, month COLLATE utf8mb4_bin)
  ) / LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY sbTickerSymbol ORDER BY CASE WHEN month COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, month COLLATE utf8mb4_bin) AS momc
FROM _t0
