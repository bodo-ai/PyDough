WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbdpclose,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    SUM(sbdpclose) AS sum_sbdpclose,
    sbdptickerid AS ticker_id,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdpdate) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbdailyprice
  GROUP BY
    sbdptickerid,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdpdate) AS INTEGER), (
          2 * -1
        ))
      END
    )
), _t1 AS (
  SELECT
    MAX(_s0.max_high) AS max_high,
    MIN(_s0.min_low) AS min_low,
    SUM(_s0.count_sbdpclose) AS sum_count_sbdpclose,
    SUM(_s0.sum_sbdpclose) AS sum_sum_sbdpclose,
    sbticker.sbtickersymbol AS symbol,
    _s0.month
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.ticker_id = sbticker.sbtickerid
  GROUP BY
    sbticker.sbtickersymbol,
    _s0.month
)
SELECT
  symbol,
  month,
  CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose AS avg_close,
  max_high,
  min_low,
  CAST((
    (
      CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose
    ) - LAG((
      CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose
    ), 1) OVER (PARTITION BY symbol ORDER BY month)
  ) AS REAL) / LAG((
    CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose
  ), 1) OVER (PARTITION BY symbol ORDER BY month) AS momc
FROM _t1
