WITH _s2 AS (
  SELECT
    SUM(sbdpclose) AS expr_0,
    COUNT(sbdpclose) AS expr_1,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbdpdate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbdpdate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbdpdate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbdpdate)), (
          2 * -1
        ))
      END
    ) AS month,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  GROUP BY
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbdpdate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbdpdate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbdpdate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbdpdate)), (
          2 * -1
        ))
      END
    ),
    sbdptickerid
), _t1 AS (
  SELECT
    SUM(_s2.expr_0) AS expr_0,
    SUM(_s2.expr_1) AS expr_1,
    MAX(_s2.max_high) AS max_high,
    MIN(_s2.min_low) AS min_low,
    _s2.month,
    _s1.sbtickersymbol AS symbol
  FROM _s2 AS _s2
  JOIN main.sbticker AS _s1
    ON _s1.sbtickerid = _s2.ticker_id
  GROUP BY
    _s2.month,
    _s1.sbtickersymbol
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
