WITH _t1 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbDailyPrice.sbdpdate AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(sbDailyPrice.sbdpdate AS DATETIME)), 2, '0')
    ) AS month,
    ANY_VALUE(sbTicker.sbtickersymbol) AS anything_sbTickerSymbol,
    COUNT(sbDailyPrice.sbdpclose) AS count_sbDpClose,
    MAX(sbDailyPrice.sbdphigh) AS max_sbDpHigh,
    MIN(sbDailyPrice.sbdplow) AS min_sbDpLow,
    SUM(sbDailyPrice.sbdpclose) AS sum_sbDpClose
  FROM main.sbDailyPrice AS sbDailyPrice
  JOIN main.sbTicker AS sbTicker
    ON sbDailyPrice.sbdptickerid = sbTicker.sbtickerid
  GROUP BY
    sbDailyPrice.sbdptickerid,
    1
), _t0 AS (
  SELECT
    anything_sbTickerSymbol,
    month,
    MAX(max_sbDpHigh) AS max_max_sbDpHigh,
    MIN(min_sbDpLow) AS min_min_sbDpLow,
    SUM(count_sbDpClose) AS sum_count_sbDpClose,
    SUM(sum_sbDpClose) AS sum_sum_sbDpClose
  FROM _t1
  GROUP BY
    1,
    2
)
SELECT
  anything_sbTickerSymbol AS symbol,
  month,
  sum_sum_sbDpClose / sum_count_sbDpClose AS avg_close,
  max_max_sbDpHigh AS max_high,
  min_min_sbDpLow AS min_low,
  (
    (
      sum_sum_sbDpClose / sum_count_sbDpClose
    ) - LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY anything_sbTickerSymbol ORDER BY CASE WHEN month COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, month COLLATE utf8mb4_bin)
  ) / LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY anything_sbTickerSymbol ORDER BY CASE WHEN month COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, month COLLATE utf8mb4_bin) AS momc
FROM _t0
