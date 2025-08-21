WITH _t1 AS (
  SELECT
    COUNT(sbdailyprice.sbdpclose) AS count_sbdpclose,
    MAX(sbdailyprice.sbdphigh) AS max_high,
    MIN(sbdailyprice.sbdplow) AS min_low,
    MAX(sbticker.sbtickersymbol) AS sbtickersymbol,
    SUM(sbdailyprice.sbdpclose) AS sum_sbdpclose,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdailyprice.sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month
  FROM main.sbdailyprice AS sbdailyprice
  JOIN main.sbticker AS sbticker
    ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  GROUP BY
    sbdailyprice.sbdptickerid,
    6
), _t0 AS (
  SELECT
    MAX(max_high) AS max_high,
    MIN(min_low) AS min_low,
    SUM(count_sbdpclose) AS sum_count_sbdpclose,
    SUM(sum_sbdpclose) AS sum_sum_sbdpclose,
    month,
    sbtickersymbol
  FROM _t1
  GROUP BY
    5,
    6
)
SELECT
  sbtickersymbol AS symbol,
  month,
  CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose AS avg_close,
  max_high,
  min_low,
  CAST((
    (
      CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose
    ) - LAG(CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month)
  ) AS REAL) / LAG(CAST(sum_sum_sbdpclose AS REAL) / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month) AS momc
FROM _t0
