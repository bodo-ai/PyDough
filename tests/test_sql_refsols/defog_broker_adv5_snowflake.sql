WITH _t1 AS (
  SELECT
    CONCAT_WS(
      '-',
      YEAR(CAST(sbdailyprice.sbdpdate AS TIMESTAMP)),
      LPAD(MONTH(CAST(sbdailyprice.sbdpdate AS TIMESTAMP)), 2, '0')
    ) AS month,
    ANY_VALUE(sbticker.sbtickersymbol) AS anything_sbtickersymbol,
    COUNT(sbdailyprice.sbdpclose) AS count_sbdpclose,
    MAX(sbdailyprice.sbdphigh) AS max_sbdphigh,
    MIN(sbdailyprice.sbdplow) AS min_sbdplow,
    SUM(sbdailyprice.sbdpclose) AS sum_sbdpclose
  FROM main.sbdailyprice AS sbdailyprice
  JOIN main.sbticker AS sbticker
    ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  GROUP BY
    sbdailyprice.sbdptickerid,
    1
), _t0 AS (
  SELECT
    anything_sbtickersymbol,
    month,
    MAX(max_sbdphigh) AS max_max_sbdphigh,
    MIN(min_sbdplow) AS min_min_sbdplow,
    SUM(count_sbdpclose) AS sum_count_sbdpclose,
    SUM(sum_sbdpclose) AS sum_sum_sbdpclose
  FROM _t1
  GROUP BY
    1,
    2
)
SELECT
  anything_sbtickersymbol AS symbol,
  month,
  sum_sum_sbdpclose / sum_count_sbdpclose AS avg_close,
  max_max_sbdphigh AS max_high,
  min_min_sbdplow AS min_low,
  (
    (
      sum_sum_sbdpclose / sum_count_sbdpclose
    ) - LAG(sum_sum_sbdpclose / sum_count_sbdpclose, 1) OVER (PARTITION BY anything_sbtickersymbol ORDER BY month)
  ) / LAG(sum_sum_sbdpclose / sum_count_sbdpclose, 1) OVER (PARTITION BY anything_sbtickersymbol ORDER BY month) AS momc
FROM _t0
