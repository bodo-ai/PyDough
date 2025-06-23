WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbdpclose,
    MAX(sbdphigh) AS max_sbdphigh,
    MIN(sbdplow) AS min_sbdplow,
    SUM(sbdpclose) AS sum_sbdpclose,
    sbdptickerid AS ticker_id,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdpdate AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbdpdate AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(sbdpdate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM CAST(sbdpdate AS DATETIME))), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbdailyprice
  GROUP BY
    sbdptickerid,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdpdate AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbdpdate AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(sbdpdate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM CAST(sbdpdate AS DATETIME))), (
          2 * -1
        ))
      END
    )
), _t1 AS (
  SELECT
    MAX(_s0.max_sbdphigh) AS max_max_sbdphigh,
    MIN(_s0.min_sbdplow) AS min_min_sbdplow,
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
  sum_sum_sbdpclose / sum_count_sbdpclose AS avg_close,
  max_max_sbdphigh AS max_high,
  min_min_sbdplow AS min_low,
  (
    (
      sum_sum_sbdpclose / sum_count_sbdpclose
    ) - LAG((
      sum_sum_sbdpclose / sum_count_sbdpclose
    ), 1) OVER (PARTITION BY symbol ORDER BY month NULLS LAST)
  ) / LAG((
    sum_sum_sbdpclose / sum_count_sbdpclose
  ), 1) OVER (PARTITION BY symbol ORDER BY month NULLS LAST) AS momc
FROM _t1
