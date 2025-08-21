WITH _t1 AS (
  SELECT
    COUNT(sbDailyPrice.sbdpclose) AS count_sbDpClose,
    MAX(sbDailyPrice.sbdphigh) AS max_high,
    MIN(sbDailyPrice.sbdplow) AS min_low,
    ANY_VALUE(sbTicker.sbtickersymbol) AS sbTickerSymbol,
    SUM(sbDailyPrice.sbdpclose) AS sum_sbDpClose,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbDailyPrice.sbdpdate AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(sbDailyPrice.sbdpdate AS DATETIME)), 2, '0')
    ) AS month
  FROM main.sbDailyPrice AS sbDailyPrice
  JOIN main.sbTicker AS sbTicker
    ON sbDailyPrice.sbdptickerid = sbTicker.sbtickerid
  GROUP BY
    sbDailyPrice.sbdptickerid,
    6
), _t0 AS (
  SELECT
    MAX(max_high) AS max_high,
    MIN(min_low) AS min_low,
    SUM(count_sbDpClose) AS sum_count_sbDpClose,
    SUM(sum_sbDpClose) AS sum_sum_sbDpClose,
    month,
    sbTickerSymbol
  FROM _t1
  GROUP BY
    5,
    6
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
    ) - LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY sbTickerSymbol ORDER BY CASE WHEN month COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, month COLLATE utf8mb4_bin)
  ) / LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY sbTickerSymbol ORDER BY CASE WHEN month COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, month COLLATE utf8mb4_bin) AS momc
FROM _t0
