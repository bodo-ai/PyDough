WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbDpClose,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    SUM(sbdpclose) AS sum_sbDpClose,
    sbdptickerid AS sbDpTickerId
  FROM main.sbDailyPrice
  GROUP BY
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdpdate AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(sbdpdate AS DATETIME)), 2, '0')
    ),
    5
), _t0 AS (
  SELECT
    MAX(_s0.max_high) AS max_high,
    MIN(_s0.min_low) AS min_low,
    SUM(_s0.count_sbDpClose) AS sum_count_sbDpClose,
    SUM(_s0.sum_sbDpClose) AS sum_sum_sbDpClose
  FROM _s0 AS _s0
  JOIN main.sbTicker AS sbTicker
    ON _s0.sbDpTickerId = sbTicker.sbtickerid
  GROUP BY
    sbTicker.sbtickersymbol,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(date AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(date AS DATETIME)), 2, '0')
    )
)
SELECT
  symbol_1 AS symbol,
  CONCAT_WS(
    '-',
    EXTRACT(YEAR FROM CAST(date AS DATETIME)),
    LPAD(EXTRACT(MONTH FROM CAST(date AS DATETIME)), 2, '0')
  ) AS month,
  sum_sum_sbDpClose / sum_count_sbDpClose AS avg_close,
  max_high,
  min_low,
  (
    (
      sum_sum_sbDpClose / sum_count_sbDpClose
    ) - LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY symbol_1 ORDER BY CASE
      WHEN CONCAT_WS(
        '-',
        EXTRACT(YEAR FROM CAST(date AS DATETIME)),
        LPAD(EXTRACT(MONTH FROM CAST(date AS DATETIME)), 2, '0')
      ) COLLATE utf8mb4_bin IS NULL
      THEN 1
      ELSE 0
    END, CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(date AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(date AS DATETIME)), 2, '0')
    ) COLLATE utf8mb4_bin)
  ) / LAG(sum_sum_sbDpClose / sum_count_sbDpClose, 1) OVER (PARTITION BY symbol_1 ORDER BY CASE
    WHEN CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(date AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(date AS DATETIME)), 2, '0')
    ) COLLATE utf8mb4_bin IS NULL
    THEN 1
    ELSE 0
  END, CONCAT_WS(
    '-',
    EXTRACT(YEAR FROM CAST(date AS DATETIME)),
    LPAD(EXTRACT(MONTH FROM CAST(date AS DATETIME)), 2, '0')
  ) COLLATE utf8mb4_bin) AS momc
FROM _t0
