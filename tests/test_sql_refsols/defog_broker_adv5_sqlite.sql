WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbdpclose,
    MAX(sbdphigh) AS max_sbdphigh,
    MIN(sbdplow) AS min_sbdplow,
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
    ) AS month,
    SUM(sbdpclose) AS sum_sbdpclose,
    sbdptickerid
  FROM main.sbdailyprice
  GROUP BY
    4,
    6
), _t0 AS (
  SELECT
    MAX(_s0.max_sbdphigh) AS max_max_sbdphigh,
    MIN(_s0.min_sbdplow) AS min_min_sbdplow,
    SUM(_s0.count_sbdpclose) AS sum_count_sbdpclose,
    SUM(_s0.sum_sbdpclose) AS sum_sum_sbdpclose,
    _s0.month,
    sbticker.sbtickersymbol
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.sbdptickerid = sbticker.sbtickerid
  GROUP BY
    5,
    6
)
SELECT
  sbtickersymbol AS symbol,
  month,
  CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose AS avg_close,
  max_max_sbdphigh AS max_high,
  min_min_sbdplow AS min_low,
  CAST((
    (
      CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose
    ) - LAG(CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month)
  ) AS REAL) / LAG(CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month) AS momc
FROM _t0
