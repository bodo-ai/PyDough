WITH _s0 AS (
  SELECT
    SUM(sbdpclose) AS expr_0,
    COUNT(sbdpclose) AS expr_1,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
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
    SUM(_s0.expr_0) AS expr_0,
    SUM(_s0.expr_1) AS expr_1,
    MAX(_s0.max_high) AS max_high,
    MIN(_s0.min_low) AS min_low,
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
  expr_0 / expr_1 AS avg_close,
  max_high,
  min_low,
  (
    (
      expr_0 / expr_1
    ) - LAG((
      expr_0 / expr_1
    ), 1) OVER (PARTITION BY symbol ORDER BY month NULLS LAST)
  ) / LAG((
    expr_0 / expr_1
  ), 1) OVER (PARTITION BY symbol ORDER BY month NULLS LAST) AS momc
FROM _t1
